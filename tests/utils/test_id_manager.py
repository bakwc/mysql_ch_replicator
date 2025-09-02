"""
Centralized Test ID Manager for Multi-Process Test Coordination

Provides bulletproof test ID consistency between pytest main process
and replicator subprocesses to prevent database name mismatches.
"""

import os
import uuid
import threading
import tempfile
import json
from pathlib import Path
import atexit


class TestIdManager:
    """Centralized test ID manager with multi-channel communication"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self._current_id = None
        self._state_file = None
        self._session_initialized = False
        
    def initialize_session(self):
        """Initialize session-wide test ID coordination"""
        if self._session_initialized:
            return
            
        with self._lock:
            if self._session_initialized:
                return
                
            try:
                # Create temporary state file for cross-process communication
                fd, state_file_path = tempfile.mkstemp(suffix='.testid', prefix='pytest_')
                os.close(fd)  # Close file descriptor, keep the path
                
                # Set session environment variable pointing to state file
                os.environ['PYTEST_TESTID_STATE_FILE'] = state_file_path
                self._state_file = state_file_path
                
                # Register cleanup handler
                atexit.register(self._cleanup_session)
                
                self._session_initialized = True
                print(f"Test ID coordination initialized: {state_file_path}")
                
            except Exception as e:
                print(f"WARNING: Failed to initialize test ID coordination: {e}")
    
    def set_test_id(self, test_id=None):
        """Set test ID with multi-channel persistence"""
        if test_id is None:
            test_id = uuid.uuid4().hex[:8]
            
        with self._lock:
            self._current_id = test_id
            
            # Channel 1: Environment variable (primary for subprocesses)
            os.environ['PYTEST_TEST_ID'] = test_id
            
            # Channel 2: File-based state (backup for complex scenarios)
            if self._state_file:
                try:
                    state_data = {
                        'test_id': test_id, 
                        'worker_id': self.get_worker_id(),
                        'pid': os.getpid()
                    }
                    with open(self._state_file, 'w') as f:
                        json.dump(state_data, f)
                except Exception as e:
                    print(f"WARNING: Failed to write test ID state file: {e}")
            
            # Channel 3: Thread-local storage (current process optimization)
            self._store_in_thread_local(test_id)
            
            return test_id
    
    def get_test_id(self):
        """Get test ID with comprehensive fallback hierarchy"""
        # Channel 1: Environment variable (subprocess-friendly)
        env_id = os.environ.get('PYTEST_TEST_ID')
        if env_id:
            return env_id
            
        # Channel 2: File-based state (cross-process fallback)
        state_file_path = os.environ.get('PYTEST_TESTID_STATE_FILE')
        if state_file_path and os.path.exists(state_file_path):
            try:
                with open(state_file_path, 'r') as f:
                    state_data = json.load(f)
                    test_id = state_data.get('test_id')
                    if test_id:
                        # Update environment for future calls
                        os.environ['PYTEST_TEST_ID'] = test_id
                        return test_id
            except Exception as e:
                print(f"WARNING: Failed to read test ID state file: {e}")
        
        # Channel 3: Thread-local storage (current process fallback)
        local_id = self._get_from_thread_local()
        if local_id:
            # Update environment for consistency
            os.environ['PYTEST_TEST_ID'] = local_id
            return local_id
        
        # Channel 4: Current instance state
        with self._lock:
            if self._current_id:
                os.environ['PYTEST_TEST_ID'] = self._current_id
                return self._current_id
        
        # Channel 5: Generate new ID (emergency fallback)
        print("WARNING: No test ID found in any channel - generating emergency fallback")
        return self.set_test_id()
    
    def get_worker_id(self):
        """Get pytest-xdist worker ID"""
        worker_id = os.environ.get('PYTEST_XDIST_WORKER', 'master')
        return worker_id.replace('gw', 'w')
    
    def _store_in_thread_local(self, test_id):
        """Store test ID in thread-local storage"""
        current_thread = threading.current_thread()
        current_thread.test_id = test_id
    
    def _get_from_thread_local(self):
        """Get test ID from thread-local storage"""
        current_thread = threading.current_thread()
        return getattr(current_thread, 'test_id', None)
    
    def _cleanup_session(self):
        """Clean up session resources"""
        if self._state_file and os.path.exists(self._state_file):
            try:
                os.unlink(self._state_file)
            except Exception:
                pass  # Ignore cleanup errors
        
        # Clean up environment
        os.environ.pop('PYTEST_TEST_ID', None)
        os.environ.pop('PYTEST_TESTID_STATE_FILE', None)
    
    def debug_status(self):
        """Return debug information about current test ID state"""
        return {
            'environment': os.environ.get('PYTEST_TEST_ID'),
            'thread_local': self._get_from_thread_local(),
            'instance_state': self._current_id,
            'worker_id': self.get_worker_id(),
            'state_file': self._state_file,
            'session_initialized': self._session_initialized,
            'pid': os.getpid()
        }


# Singleton instance for global coordination
_test_id_manager = TestIdManager()


def get_test_id_manager():
    """Get the singleton test ID manager instance"""
    return _test_id_manager


def reset_test_id():
    """Reset test ID for new test (convenience function)"""
    return _test_id_manager.set_test_id()


def get_current_test_id():
    """Get current test ID (convenience function)"""
    return _test_id_manager.get_test_id()


def initialize_test_coordination():
    """Initialize session-level test coordination (convenience function)"""
    _test_id_manager.initialize_session()


def get_worker_id():
    """Get current worker ID (convenience function)"""
    return _test_id_manager.get_worker_id()