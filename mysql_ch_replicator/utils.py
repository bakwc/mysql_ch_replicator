import signal
import subprocess
import os
import sys
import time
import tempfile
import shlex

from pathlib import Path
from logging import getLogger


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
        logger.info(f'{self.proc_name} stopped')
        sys.exit(0)


class ProcessRunner:
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.log_file = None
        
    def run(self):
        # Use shlex for proper command parsing instead of simple split
        try:
            cmd = shlex.split(self.cmd) if isinstance(self.cmd, str) else self.cmd
        except ValueError as e:
            logger.error(f"Failed to parse command '{self.cmd}': {e}")
            cmd = self.cmd.split()  # Fallback to simple split
        
        try:
            # Create temporary log file to prevent subprocess deadlock
            self.log_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, 
                                                       prefix='replicator_', suffix='.log')
            
            # CRITICAL: Prepare environment with explicit test ID inheritance
            subprocess_env = os.environ.copy()
            
            # Ensure test ID is available for subprocess isolation
            test_id = subprocess_env.get('PYTEST_TEST_ID')
            if not test_id:
                # Try to get from state file as fallback
                state_file = subprocess_env.get('PYTEST_TESTID_STATE_FILE')
                if state_file and os.path.exists(state_file):
                    try:
                        import json
                        with open(state_file, 'r') as f:
                            state_data = json.load(f)
                            test_id = state_data.get('test_id')
                            if test_id:
                                subprocess_env['PYTEST_TEST_ID'] = test_id
                                logger.debug(f"ProcessRunner: Retrieved test ID from state file: {test_id}")
                    except Exception as e:
                        logger.warning(f"ProcessRunner: Failed to read test ID from state file: {e}")
                
                # Last resort - generate one but warn
                if not test_id:
                    import uuid
                    test_id = uuid.uuid4().hex[:8]
                    subprocess_env['PYTEST_TEST_ID'] = test_id
                    logger.warning(f"ProcessRunner: Generated emergency test ID {test_id} for subprocess")
            
            # Debug logging for environment verification
            test_related_vars = {k: v for k, v in subprocess_env.items() if 'TEST' in k or 'PYTEST' in k}
            if test_related_vars:
                logger.debug(f"ProcessRunner environment for {self.cmd}: {test_related_vars}")
            
            # Prevent subprocess deadlock by redirecting to files instead of PIPE
            # and use start_new_session for better process isolation
            self.process = subprocess.Popen(
                cmd,
                env=subprocess_env,  # CRITICAL: Explicit environment passing
                stdout=self.log_file,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                universal_newlines=True,
                start_new_session=True,  # Process isolation - prevents signal propagation
                cwd=os.getcwd()  # Explicit working directory
            )
            self.log_file.flush()
            logger.debug(f"Started process {self.process.pid}: {self.cmd}")
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

    def restart_dead_process_if_required(self):
        if self.process is None:
            logger.warning(f'Restarting stopped process: < {self.cmd} >')
            self.run()
            return

        res = self.process.poll()
        if res is None:
            # Process is running fine.
            return

        # Read log file for debugging instead of using communicate() to avoid deadlock
        log_content = ""
        if self.log_file:
            try:
                self.log_file.close()
                with open(self.log_file.name, 'r') as f:
                    log_content = f.read().strip()
                # Clean up old log file
                os.unlink(self.log_file.name)
            except Exception as e:
                logger.debug(f"Could not read process log: {e}")
            finally:
                self.log_file = None

        logger.warning(f'Process dead (exit code: {res}), restarting: < {self.cmd} >')
        if log_content:
            # Show last few lines of log for debugging
            lines = log_content.split('\n')
            last_lines = lines[-5:] if len(lines) > 5 else lines
            logger.error(f'Process last output: {" | ".join(last_lines)}')
        
        self.run()

    def stop(self):
        if self.process is not None:
            try:
                # Send SIGINT first for graceful shutdown
                self.process.send_signal(signal.SIGINT)
                # Wait with timeout to avoid hanging
                try:
                    self.process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    # Force kill if graceful shutdown fails
                    logger.warning(f"Process {self.process.pid} did not respond to SIGINT, using SIGKILL")
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
