"""
Dynamic Configuration Manager for Test Database Isolation

Provides centralized, DRY utilities for creating isolated test configurations
that ensure complete database isolation for parallel test execution.
"""

import os
import tempfile
import threading
import uuid
from typing import Dict, Optional, Any
import yaml
from tests.utils.test_id_manager import get_test_id_manager

# Get the centralized test ID manager
_test_id_manager = get_test_id_manager()

# Legacy globals kept for compatibility during transition
_global_test_state = {
    'test_id': None,
    'lock': threading.Lock()
}
_config_local = threading.local()


class DynamicConfigManager:
    """Centralized manager for dynamic test configuration with complete database isolation"""
    
    def __init__(self):
        self._temp_files = []  # Track temp files for cleanup
    
    def get_worker_id(self) -> str:
        """Get pytest-xdist worker ID for database isolation"""
        worker_id = os.environ.get('PYTEST_XDIST_WORKER', 'master')
        return worker_id.replace('gw', 'w')  # gw0 -> w0, gw1 -> w1, etc.
    
    def get_test_id(self) -> str:
        """Get unique test identifier using centralized manager"""
        return _test_id_manager.get_test_id()
    
    def reset_test_id(self):
        """Reset test ID for new test using centralized manager"""
        old_id = _test_id_manager.get_test_id() if hasattr(_test_id_manager, '_current_id') else None
        new_id = _test_id_manager.set_test_id()
        
        # Legacy compatibility - update old globals
        with _global_test_state['lock']:
            _global_test_state['test_id'] = new_id
            _config_local.test_id = new_id
        
        # Minimal debug output for test ID coordination
        if old_id != new_id:
            print(f"Test ID: {old_id} → {new_id}")
        return new_id
    
    def get_isolated_database_name(self, suffix: str = "") -> str:
        """Generate isolated database name (source database)"""
        worker_id = self.get_worker_id()
        test_id = self.get_test_id()
        return f"test_db_{worker_id}_{test_id}{suffix}"
    
    def get_isolated_table_name(self, suffix: str = "") -> str:
        """Generate isolated table name"""
        worker_id = self.get_worker_id()
        test_id = self.get_test_id()
        return f"test_table_{worker_id}_{test_id}{suffix}"
    
    def get_isolated_target_database_name(self, source_db_name: str, target_suffix: str = "target") -> str:
        """Generate isolated target database name for replication mapping"""
        worker_id = self.get_worker_id()
        test_id = self.get_test_id()
        return f"{target_suffix}_{worker_id}_{test_id}"
    
    def get_isolated_data_dir(self, suffix: str = "") -> str:
        """Generate isolated data directory path in organized binlog folder"""
        worker_id = self.get_worker_id()
        test_id = self.get_test_id()
        return f"/app/binlog/{worker_id}_{test_id}{suffix}"
    
    def create_isolated_target_mappings(self, source_databases: list, target_prefix: str = "target") -> Dict[str, str]:
        """
        Create dynamic target database mappings for isolated parallel testing
        
        Args:
            source_databases: List of source database names (can be static or dynamic)
            target_prefix: Prefix for target database names
            
        Returns:
            Dict mapping source -> isolated target database names
        """
        mappings = {}
        worker_id = self.get_worker_id()
        test_id = self.get_test_id()
        
        for i, source_db in enumerate(source_databases):
            # If source is already dynamic (contains worker/test ID), use as-is
            if f"_{worker_id}_{test_id}" in source_db:
                source_key = source_db
            else:
                # Convert static source to dynamic
                source_key = f"test_db_{worker_id}_{test_id}" if source_db.startswith("test_db") else source_db
            
            # Create isolated target
            target_db = f"{target_prefix}_{worker_id}_{test_id}"
            if i > 0:  # Add index for multiple mappings
                target_db += f"_{i}"
                
            mappings[source_key] = target_db
        
        return mappings
    
    def create_dynamic_config(
        self, 
        base_config_path: str, 
        target_mappings: Optional[Dict[str, str]] = None,
        custom_settings: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a dynamic configuration file with complete database isolation
        
        Args:
            base_config_path: Path to base configuration file
            target_mappings: Custom target database mappings (optional)
            custom_settings: Additional custom configuration settings (optional)
            
        Returns:
            Path to temporary configuration file (automatically cleaned up)
        """
        # Load base configuration
        with open(base_config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        # Apply isolated data directory and ensure parent directory exists
        isolated_data_dir = self.get_isolated_data_dir()
        config_dict['binlog_replicator']['data_dir'] = isolated_data_dir
        
        # CRITICAL FIX: Ensure parent directory exists to prevent process startup failures
        parent_dir = os.path.dirname(isolated_data_dir)  # e.g. /app/binlog
        try:
            os.makedirs(parent_dir, exist_ok=True)
            print(f"DEBUG: Ensured parent directory exists: {parent_dir}")
        except Exception as e:
            print(f"WARNING: Could not create parent directory {parent_dir}: {e}")
        
        # Apply custom settings FIRST so they can override target database mapping logic
        if custom_settings:
            self._deep_update(config_dict, custom_settings)
        
        # Apply dynamic target database mappings (but respect custom_settings overrides)
        if target_mappings:
            config_dict['target_databases'] = target_mappings
        elif 'target_databases' in config_dict and config_dict['target_databases']:
            # Convert existing static mappings to dynamic (only if not cleared by custom_settings)
            existing_mappings = config_dict['target_databases']
            dynamic_mappings = {}
            
            for source, target in existing_mappings.items():
                # Convert source to dynamic if needed
                if 'test_db' in source or source.startswith('replication-'):
                    dynamic_source = self.get_isolated_database_name()
                else:
                    dynamic_source = source
                
                # Convert target to dynamic
                dynamic_target = self.get_isolated_target_database_name(source, target)
                dynamic_mappings[dynamic_source] = dynamic_target
            
            config_dict['target_databases'] = dynamic_mappings
        else:
            # Ensure empty target_databases for consistency
            config_dict['target_databases'] = {}
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
        try:
            yaml.dump(config_dict, temp_file, default_flow_style=False)
            temp_file.flush()
            self._temp_files.append(temp_file.name)
            return temp_file.name
        finally:
            temp_file.close()
    
    def _deep_update(self, base_dict: dict, update_dict: dict):
        """Deep update dictionary (modifies base_dict in place)"""
        for key, value in update_dict.items():
            if key in base_dict and isinstance(base_dict[key], dict) and isinstance(value, dict) and value:
                # Only merge dicts if the update value is non-empty
                # Empty dicts ({}) should replace the entire key, not merge
                self._deep_update(base_dict[key], value)
            else:
                base_dict[key] = value
    
    def cleanup_temp_files(self):
        """Clean up all temporary configuration files"""
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            except Exception:
                pass  # Ignore cleanup errors
        self._temp_files.clear()
    
    def __del__(self):
        """Automatic cleanup on object destruction"""
        self.cleanup_temp_files()


# Singleton instance for consistent usage across tests
_config_manager = DynamicConfigManager()


def get_config_manager() -> DynamicConfigManager:
    """Get the singleton configuration manager instance"""
    return _config_manager


# Convenience functions for backward compatibility and ease of use
def get_isolated_database_name(suffix: str = "") -> str:
    """Get isolated database name (convenience function)"""
    return _config_manager.get_isolated_database_name(suffix)


def get_isolated_table_name(suffix: str = "") -> str:
    """Get isolated table name (convenience function)"""
    return _config_manager.get_isolated_table_name(suffix)


def get_isolated_data_dir(suffix: str = "") -> str:
    """Get isolated data directory (convenience function)"""
    return _config_manager.get_isolated_data_dir(suffix)


def create_dynamic_config(
    base_config_path: str,
    target_mappings: Optional[Dict[str, str]] = None,
    custom_settings: Optional[Dict[str, Any]] = None
) -> str:
    """Create dynamic config file (convenience function)"""
    return _config_manager.create_dynamic_config(base_config_path, target_mappings, custom_settings)


def reset_test_isolation():
    """Reset test isolation using centralized manager (convenience function for fixtures)"""
    return _config_manager.reset_test_id()


def cleanup_config_files():
    """Clean up temporary config files (convenience function)"""
    _config_manager.cleanup_temp_files()