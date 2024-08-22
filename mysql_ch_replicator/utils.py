import signal
import subprocess

from logging import getLogger


logger = getLogger(__name__)

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


class ProcessRunner:
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self):
        cmd = self.cmd.split()
        self.process = subprocess.Popen(cmd)

    def restart_dead_process_if_required(self):
        res = self.process.poll()
        if res is None:
            # still running
            return
        logger.warning(f'Restarting dead process: < {self.cmd} >')
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