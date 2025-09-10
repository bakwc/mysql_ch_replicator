import os
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
from logging import getLogger
from pathlib import Path

logger = getLogger(__name__)


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


class RegularKiller:
    def __init__(self, proc_name):
        self.proc_name = proc_name
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        logger.info(f"{self.proc_name} stopped")
        sys.exit(0)


class ProcessRunner:
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.log_file = None
        self.log_forwarding_thread = None
        self.should_stop_forwarding = False
    
    def _forward_logs(self):
        """Forward subprocess logs to the main process logger in real-time."""
        if not self.log_file or not hasattr(self.log_file, 'name'):
            return
            
        log_path = self.log_file.name
        last_position = 0
        
        # Extract process name from command for logging prefix
        cmd_parts = self.cmd.split()
        process_name = "subprocess"
        if len(cmd_parts) > 0:
            if "binlog_replicator" in self.cmd:
                process_name = "binlogrepl"
            elif "db_replicator" in self.cmd and "--db" in cmd_parts:
                try:
                    db_index = cmd_parts.index("--db") + 1
                    if db_index < len(cmd_parts):
                        db_name = cmd_parts[db_index]
                        process_name = f"dbrepl {db_name}"
                except (ValueError, IndexError):
                    process_name = "dbrepl"
            elif "db_optimizer" in self.cmd:
                process_name = "dbopt"
        
        while not self.should_stop_forwarding:
            try:
                if os.path.exists(log_path):
                    with open(log_path, 'r') as f:
                        f.seek(last_position)
                        new_content = f.read()
                        if new_content:
                            # Forward each line to main logger with subprocess prefix
                            lines = new_content.strip().split('\n')
                            for line in lines:
                                if line.strip():
                                    # Remove timestamp and level from subprocess log to avoid duplication
                                    # Format: [tag timestamp level] message -> message
                                    clean_line = line
                                    if '] ' in line:
                                        bracket_end = line.find('] ')
                                        if bracket_end != -1:
                                            clean_line = line[bracket_end + 2:]
                                    
                                    # Only forward important log messages to avoid spam
                                    # Forward stats, errors, warnings, and key info messages
                                    if any(keyword in clean_line.lower() for keyword in 
                                          ['stats:', 'ch_stats:', 'error', 'warning', 'failed', 'last transaction', 
                                           'processed events', 'connection', 'replication', 'events_count',
                                           'insert_events_count', 'erase_events_count']):
                                        logger.info(f"[{process_name}] {clean_line}")
                        
                        last_position = f.tell()
                
                time.sleep(2)  # Check for new logs every 2 seconds to reduce overhead
                
            except Exception as e:
                logger.debug(f"Error forwarding logs for {process_name}: {e}")
                time.sleep(2)

    def run(self):
        """
        Start the subprocess with proper environment isolation.

        IMPORTANT: This method includes test isolation logic that ONLY runs during
        pytest execution. In production, no test-related environment variables
        are set or required. If you see "emergency test ID" warnings in production,
        do NOT remove the is_testing conditional - the issue is elsewhere.

        The test isolation prevents database conflicts during parallel test execution
        but should never interfere with production operations.
        """
        # Use shlex for proper command parsing instead of simple split
        try:
            cmd = shlex.split(self.cmd) if isinstance(self.cmd, str) else self.cmd
        except ValueError as e:
            logger.error(f"Failed to parse command '{self.cmd}': {e}")
            cmd = self.cmd.split()  # Fallback to simple split

        try:
            # Create temporary log file to prevent subprocess deadlock
            self.log_file = tempfile.NamedTemporaryFile(
                mode="w+", delete=False, prefix="replicator_", suffix=".log"
            )

            # Prepare environment for subprocess
            subprocess_env = os.environ.copy()

            # CRITICAL: Test ID logic should ONLY run during testing, NOT in production
            #
            # BACKGROUND: The test isolation system was designed to prevent database conflicts
            # during parallel pytest execution. However, the original implementation had a bug
            # where it ALWAYS tried to generate test IDs, even in production environments.
            #
            # PRODUCTION PROBLEM: In production, no PYTEST_TEST_ID exists, so the code would
            # always generate "emergency test IDs" and log confusing warnings like:
            # "ProcessRunner: Generated emergency test ID 3e345c30 for subprocess"
            #
            # SOLUTION: Only run test ID logic when actually running under pytest.
            # This prevents production noise while preserving test isolation functionality.
            #
            # DO NOT REVERT: If you see test ID warnings in production, the fix is NOT
            # to make this logic always run - it's to ensure this conditional stays in place.
            is_testing = (
                any(
                    key in subprocess_env
                    for key in ["PYTEST_CURRENT_TEST", "PYTEST_XDIST_WORKER"]
                )
                or "pytest" in sys.modules
            )

            if is_testing:
                # Ensure test ID is available for subprocess isolation during tests
                test_id = subprocess_env.get("PYTEST_TEST_ID")
                if not test_id:
                    # Try to get from state file as fallback
                    state_file = subprocess_env.get("PYTEST_TESTID_STATE_FILE")
                    if state_file and os.path.exists(state_file):
                        try:
                            import json

                            with open(state_file, "r") as f:
                                state_data = json.load(f)
                                test_id = state_data.get("test_id")
                                if test_id:
                                    subprocess_env["PYTEST_TEST_ID"] = test_id
                                    logger.debug(
                                        f"ProcessRunner: Retrieved test ID from state file: {test_id}"
                                    )
                        except Exception as e:
                            logger.warning(
                                f"ProcessRunner: Failed to read test ID from state file: {e}"
                            )

                    # Last resort - generate one but warn
                    if not test_id:
                        import uuid

                        test_id = uuid.uuid4().hex[:8]
                        subprocess_env["PYTEST_TEST_ID"] = test_id
                        logger.warning(
                            f"ProcessRunner: Generated emergency test ID {test_id} for subprocess"
                        )

                # Debug logging for environment verification
                test_related_vars = {
                    k: v
                    for k, v in subprocess_env.items()
                    if "TEST" in k or "PYTEST" in k
                }
                if test_related_vars:
                    logger.debug(
                        f"ProcessRunner environment for {self.cmd}: {test_related_vars}"
                    )

            # Prevent subprocess deadlock by redirecting to files instead of PIPE
            # and use start_new_session for better process isolation
            self.process = subprocess.Popen(
                cmd,
                env=subprocess_env,  # CRITICAL: Explicit environment passing
                stdout=self.log_file,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                universal_newlines=True,
                start_new_session=True,  # Process isolation - prevents signal propagation
                cwd=os.getcwd(),  # Explicit working directory
            )
            self.log_file.flush()
            logger.debug(f"Started process {self.process.pid}: {self.cmd}")
            
            # Start log forwarding thread
            self.should_stop_forwarding = False
            self.log_forwarding_thread = threading.Thread(
                target=self._forward_logs,
                daemon=True,
                name=f"LogForwarder-{self.process.pid}"
            )
            self.log_forwarding_thread.start()
            
        except Exception as e:
            if self.log_file:
                self.log_file.close()
                try:
                    os.unlink(self.log_file.name)
                except:
                    pass
                self.log_file = None
            logger.error(f"Failed to start process '{self.cmd}': {e}")
            raise

    def _read_log_output(self):
        """Read current log output for debugging"""
        if not self.log_file or not hasattr(self.log_file, "name"):
            return "No log file available"

        try:
            # Close and reopen to read current contents
            log_path = self.log_file.name
            if os.path.exists(log_path):
                with open(log_path, "r") as f:
                    content = f.read().strip()
                    return content if content else "No output captured"
            else:
                return "Log file does not exist"
        except Exception as e:
            return f"Error reading log: {e}"

    def restart_dead_process_if_required(self):
        if self.process is None:
            logger.warning(f"Restarting stopped process: < {self.cmd} >")
            self.run()
            return

        res = self.process.poll()
        if res is None:
            # Process is running fine.
            return

        # Stop log forwarding thread for dead process
        self.should_stop_forwarding = True
        if self.log_forwarding_thread and self.log_forwarding_thread.is_alive():
            try:
                self.log_forwarding_thread.join(timeout=2.0)
            except Exception as e:
                logger.debug(f"Error joining log forwarding thread during restart: {e}")

        # Read log file for debugging instead of using communicate() to avoid deadlock
        log_content = ""
        if self.log_file:
            try:
                self.log_file.close()
                with open(self.log_file.name, "r") as f:
                    log_content = f.read().strip()
                # Clean up old log file
                os.unlink(self.log_file.name)
            except Exception as e:
                logger.debug(f"Could not read process log: {e}")
            finally:
                self.log_file = None

        logger.warning(f"Process dead (exit code: {res}), restarting: < {self.cmd} >")
        if log_content:
            # Show last few lines of log for debugging
            lines = log_content.split("\n")
            last_lines = lines[-5:] if len(lines) > 5 else lines
            logger.error(f"Process last output: {' | '.join(last_lines)}")

        self.run()

    def stop(self):
        # Stop log forwarding thread first
        self.should_stop_forwarding = True
        if self.log_forwarding_thread and self.log_forwarding_thread.is_alive():
            try:
                self.log_forwarding_thread.join(timeout=2.0)
            except Exception as e:
                logger.debug(f"Error joining log forwarding thread: {e}")
        
        if self.process is not None:
            try:
                # Send SIGINT first for graceful shutdown
                self.process.send_signal(signal.SIGINT)
                # Wait with timeout to avoid hanging
                try:
                    self.process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    # Force kill if graceful shutdown fails
                    logger.warning(
                        f"Process {self.process.pid} did not respond to SIGINT, using SIGKILL"
                    )
                    self.process.kill()
                    self.process.wait()
            except Exception as e:
                logger.warning(f"Error stopping process: {e}")
            finally:
                self.process = None

        # Clean up log file
        if self.log_file:
            try:
                self.log_file.close()
                os.unlink(self.log_file.name)
            except Exception as e:
                logger.debug(f"Could not clean up log file: {e}")
            finally:
                self.log_file = None

    def wait_complete(self):
        if self.process is not None:
            self.process.wait()
            self.process = None

        # Stop log forwarding thread
        self.should_stop_forwarding = True
        if self.log_forwarding_thread and self.log_forwarding_thread.is_alive():
            try:
                self.log_forwarding_thread.join(timeout=2.0)
            except Exception as e:
                logger.debug(f"Error joining log forwarding thread: {e}")

        # Clean up log file
        if self.log_file:
            try:
                self.log_file.close()
                os.unlink(self.log_file.name)
            except Exception as e:
                logger.debug(f"Could not clean up log file: {e}")
            finally:
                self.log_file = None

    def __del__(self):
        self.stop()


def touch_all_files(directory_path):
    dir_path = Path(directory_path)

    if not dir_path.exists():
        raise FileNotFoundError(f"The directory '{directory_path}' does not exist.")

    if not dir_path.is_dir():
        raise NotADirectoryError(f"The path '{directory_path}' is not a directory.")

    current_time = time.time()

    for item in dir_path.iterdir():
        if item.is_file():
            try:
                # Update the modification and access times
                os.utime(item, times=(current_time, current_time))
            except Exception as e:
                logger.warning(f"Failed to touch {item}: {e}")


def format_floats(data):
    if isinstance(data, dict):
        return {k: format_floats(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [format_floats(v) for v in data]
    elif isinstance(data, float):
        return round(data, 3)
    return data
