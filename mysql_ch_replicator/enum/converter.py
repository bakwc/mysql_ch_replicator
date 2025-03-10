from typing import List, Union, Optional, Any
from logging import getLogger

# Create a single module-level logger
logger = getLogger(__name__)

class EnumConverter:
    """Class to handle conversion of enum values between MySQL and ClickHouse"""
    
    @staticmethod
    def convert_mysql_to_clickhouse_enum(
        value: Any, 
        enum_values: List[str],
        field_name: str = "unknown"
    ) -> Optional[Union[str, int]]:
        """
        Convert a MySQL enum value to the appropriate ClickHouse representation
        
        Args:
            value: The MySQL enum value (can be int, str, None)
            enum_values: List of possible enum string values
            field_name: Name of the field (for better error reporting)
            
        Returns:
            The properly converted enum value for ClickHouse
        """
        # Handle NULL values
        if value is None:
            return None
            
        # Handle integer values (index-based)
        if isinstance(value, int):
            # Check if the value is 0
            if value == 0:
                # Return 0 as-is - let ClickHouse handle it according to the field's nullability
                logger.debug(f"ENUM CONVERSION: Found enum index 0 for field '{field_name}'. Keeping as 0.")
                return 0
                
            # Validate that the enum index is within range
            if value < 1 or value > len(enum_values):
                # Log the issue
                logger.error(f"ENUM CONVERSION: Invalid enum index {value} for field '{field_name}' "
                      f"with values {enum_values}")
                # Return the value unchanged
                return value
            else:
                # Convert to the string representation (lowercase to match our new convention)
                return enum_values[int(value)-1].lower()
                
        # Handle string values
        elif isinstance(value, str):
            # Validate that the string value exists in enum values
            # First check case-sensitive, then case-insensitive
            if value in enum_values:
                return value.lower()
            
            # Try case-insensitive match
            lowercase_enum_values = [v.lower() for v in enum_values]
            if value.lower() in lowercase_enum_values:
                return value.lower()
                
            # Value not found in enum values
            logger.error(f"ENUM CONVERSION: Invalid enum value '{value}' not in {enum_values} "
                  f"for field '{field_name}'")
            # Return the value unchanged
            return value
            
        # Handle any other unexpected types
        else:
            logger.error(f"ENUM CONVERSION: Unexpected type {type(value)} for enum field '{field_name}'")
            # Return the value unchanged
            return value 