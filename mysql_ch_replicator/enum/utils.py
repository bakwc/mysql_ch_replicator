from typing import List, Optional, Tuple

def find_enum_definition_end(text: str, start_pos: int) -> int:
    """
    Find the end position of an enum definition in a string
    
    Args:
        text: The input text containing the enum definition
        start_pos: The starting position (after 'enum(')
    
    Returns:
        int: The position of the closing parenthesis
    """
    open_parens = 1
    in_quotes = False
    quote_char = None
    
    for i in range(start_pos, len(text)):
        char = text[i]
        
        # Handle quote state
        if not in_quotes and char in ("'", '"', '`'):
            in_quotes = True
            quote_char = char
            continue
        elif in_quotes and char == quote_char:
            # Check for escaped quotes
            if i > 0 and text[i-1] == '\\':
                # This is an escaped quote, not the end of the quoted string
                continue
            # End of quoted string
            in_quotes = False
            quote_char = None
            continue
            
        # Only process parentheses when not in quotes
        if not in_quotes:
            if char == '(':
                open_parens += 1
            elif char == ')':
                open_parens -= 1
                if open_parens == 0:
                    return i
    
    # If we get here, the definition is malformed
    raise ValueError("Unbalanced parentheses in enum definition")


def extract_field_components(line: str) -> Tuple[str, str, List[str]]:
    """
    Extract field name, type, and parameters from a MySQL field definition line
    
    Args:
        line: A line from a field definition
        
    Returns:
        Tuple containing field_name, field_type, and parameters
    """
    components = line.split(' ')
    field_name = components[0].strip('`')
    
    # Handle special case for enum and set types that might contain spaces
    if len(components) > 1 and (
        components[1].lower().startswith('enum(') or 
        components[1].lower().startswith('set(')
    ):
        field_type_start = components[1]
        field_type_components = [field_type_start]
        
        # If the enum definition is not complete on this component
        if not _is_complete_definition(field_type_start):
            # Join subsequent components until we find the end of the definition
            for component in components[2:]:
                field_type_components.append(component)
                if ')' in component:
                    break
        
        field_type = ' '.join(field_type_components)
        parameters = components[len(field_type_components) + 1:]
    else:
        field_type = components[1] if len(components) > 1 else ""
        parameters = components[2:] if len(components) > 2 else []
    
    return field_name, field_type, parameters


def _is_complete_definition(text: str) -> bool:
    """
    Check if a string contains a complete enum definition (balanced parentheses)
    
    Args:
        text: The string to check
        
    Returns:
        bool: True if the definition is complete
    """
    open_count = text.count('(')
    close_count = text.count(')')
    return open_count > 0 and open_count == close_count 