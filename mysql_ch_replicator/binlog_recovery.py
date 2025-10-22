"""
Shared binlog recovery utilities for handling MySQL Error 1236 (binlog corruption).
"""
import os
import shutil
from logging import getLogger

logger = getLogger(__name__)


def recover_from_binlog_corruption(binlog_dir: str, error: Exception) -> None:
    """
    Recover from MySQL Error 1236 (binlog corruption) by deleting the corrupted
    binlog directory and raising an exception to trigger process restart.

    Args:
        binlog_dir: Path to the binlog directory to delete
        error: The original OperationalError that triggered recovery

    Raises:
        RuntimeError: Always raised to trigger process restart after cleanup

    This function:
    1. Logs the error and recovery attempt
    2. Deletes the corrupted binlog directory
    3. Raises RuntimeError to exit the process cleanly
    4. ProcessRunner will automatically restart the process
    5. On restart, replication resumes from a fresh state
    """
    logger.error(f"[binlogrepl] operational error (1236, 'Could not find first log file name in binary log index file')")
    logger.error(f"[binlogrepl] Full error: {error}")
    logger.info("[binlogrepl] Error 1236 detected - attempting automatic recovery")

    # Delete the corrupted binlog directory to force fresh start
    if os.path.exists(binlog_dir):
        logger.warning(f"[binlogrepl] Deleting corrupted binlog directory: {binlog_dir}")
        try:
            shutil.rmtree(binlog_dir)
            logger.info(f"[binlogrepl] Successfully deleted binlog directory: {binlog_dir}")
        except Exception as delete_error:
            logger.error(f"[binlogrepl] Failed to delete binlog directory: {delete_error}", exc_info=True)
            raise RuntimeError("Failed to delete corrupted binlog directory") from delete_error
    else:
        logger.warning(f"[binlogrepl] Binlog directory does not exist: {binlog_dir}")

    # Exit process cleanly to trigger automatic restart by runner
    logger.info("[binlogrepl] Exiting process for automatic restart by runner")
    logger.info("[binlogrepl] The runner will automatically restart this process")
    raise RuntimeError("Binlog corruption detected (Error 1236) - restarting for recovery") from error
