import signal
import subprocess
import os
import sys
import time

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

    def run(self):
        cmd = self.cmd.split()
        self.process = subprocess.Popen(cmd)

    def restart_dead_process_if_required(self):
        if self.process is None:
            logger.warning(f'Restarting stopped process: < {self.cmd} >')
            self.run()
            return

        res = self.process.poll()
        if res is None:
            # Process is running fine.
            return

        logger.warning(f'Process dead (exit code: {res}), restarting: < {self.cmd} >')
        # Process has already terminated, just reap it
        self.process.wait()
        self.run()

    def stop(self):
        if self.process is not None:
            self.process.send_signal(signal.SIGINT)
            self.process.wait()
            self.process = None

    def wait_complete(self):
        self.process.wait()
        self.process = None

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
