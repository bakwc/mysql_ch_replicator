from typing import List, Tuple, Optional, Dict, Any

def find_enum_or_set_definition_end(line: str) -> Tuple[int, str, str]:
    """
    Find the end of an enum or set definition in a DDL line
    
    Args:
        line: The DDL line containing an enum or set definition
        
    Returns:
        Tuple containing (end_position, field_type, field_parameters)
    """
    open_parens = 0
    in_quotes = False
    quote_char = None
    end_pos = -1

    for i, char in enumerate(line):
        if char in "'\"" and (i == 0 or line[i - 1] != "\\"):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
        elif char == '(' and not in_quotes:
            open_parens += 1
        elif char == ')' and not in_quotes:
            open_parens -= 1
            if open_parens == 0:
                end_pos = i + 1
                break

    if end_pos > 0:
        field_type = line[:end_pos]
        field_parameters = line[end_pos:].strip()
        return end_pos, field_type, field_parameters
    
    # Fallback to splitting by space if we can't find the end
    # Use split() instead of split(' ') to handle multiple consecutive spaces
    definition = line.split()
    field_type = definition[0] if definition else ""
    field_parameters = ' '.join(definition[1:]) if len(definition) > 1 else ''
    
    return -1, field_type, field_parameters


def parse_enum_or_set_field(line: str, field_name: str, is_backtick_quoted: bool = False) -> Tuple[str, str, str]:
    """
    Parse a field definition line containing an enum or set type
    
    Args:
        line: The line to parse
        field_name: The name of the field (already extracted)
        is_backtick_quoted: Whether the field name was backtick quoted
        
    Returns:
        Tuple containing (field_name, field_type, field_parameters)
    """
    # If the field name was backtick quoted, it's already been extracted
    if is_backtick_quoted:
        line = line.strip()
        # Don't split by space for enum and set types that might contain spaces
        if line.lower().startswith('enum(') or line.lower().startswith('set('):
            end_pos, field_type, field_parameters = find_enum_or_set_definition_end(line)
        else:
            # Use split() instead of split(' ') to handle multiple consecutive spaces
            definition = line.split()
            field_type = definition[0] if definition else ""
            field_parameters = ' '.join(definition[1:]) if len(definition) > 1 else ''
    else:
        # For non-backtick quoted fields
        # Use split() instead of split(' ') to handle multiple consecutive spaces
        definition = line.split()
        definition = definition[1:]  # Skip the field name which was already extracted
        
        if definition and (
            definition[0].lower().startswith('enum(')
            or definition[0].lower().startswith('set(')
        ):
            line = ' '.join(definition)
            end_pos, field_type, field_parameters = find_enum_or_set_definition_end(line)
        else:
            field_type = definition[0] if definition else ""
            field_parameters = ' '.join(definition[1:]) if len(definition) > 1 else ''
    
    return field_name, field_type, field_parameters


def extract_enum_or_set_values(field_type: str, from_parser_func=None) -> Optional[List[str]]:
    """
    Extract values from an enum or set field type
    
    Args:
        field_type: The field type string (e.g. "enum('a','b','c')")
        from_parser_func: Optional function to use for parsing (defaults to simple string parsing)
        
    Returns:
        List of extracted values or None if not an enum/set
    """
    if field_type.lower().startswith('enum('):
        # Use the provided parser function if available
        if from_parser_func:
            return from_parser_func(field_type)
            
        # Simple parsing fallback
        vals = field_type[len('enum('):]
        close_pos = vals.find(')')
        vals = vals[:close_pos]
        vals = vals.split(',')
        return [strip_value(v) for v in vals]
        
    elif 'set(' in field_type.lower():
        vals = field_type[field_type.lower().find('set(') + len('set('):]
        close_pos = vals.find(')')
        vals = vals[:close_pos]
        vals = vals.split(',')
        return [strip_value(v) for v in vals]
        
    return None


def strip_value(value: str) -> str:
    """
    Strip quotes from enum/set values
    
    Args:
        value: The value to strip
        
    Returns:
        Stripped value
    """
    value = value.strip()
    if not value:
        return value
    if value[0] in '"\'`':
        return value[1:-1]
    return value 