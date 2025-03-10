def parse_mysql_enum(enum_definition):
    """
    Accepts a MySQL ENUM definition string (case–insensitive),
    for example:
       enum('point','qwe','def')
       ENUM("asd", 'qwe', "def")
       enum(`point`,`qwe`,`def`)
    and returns a list of strings like:
       ['point', 'qwe', 'def']

    Note:
      - For single- and double–quoted values, backslash escapes are handled.
      - For backtick–quoted values, only doubling (``) is recognized as escaping.
    """
    # First, trim any whitespace.
    s = enum_definition.strip()

    # Check that the string begins with "enum" (case–insensitive)
    if not s[:4].lower() == "enum":
        raise ValueError("String does not start with 'enum'")

    # Find the first opening parenthesis.
    pos = s.find('(')
    if pos == -1:
        raise ValueError("Missing '(' in the enum definition")

    # Extract the text inside the outer parenthesis.
    # We use a helper to extract the contents taking into account
    # that quotes (of any supported type) and escapes may appear.
    inner_content, next_index = _extract_parenthesized_content(s, pos)
    # Optionally, you can check that only whitespace follows next_index.

    # Now parse out the comma–separated string literals.
    return _parse_enum_values(inner_content)


def _extract_parenthesized_content(s, start_index):
    """
    Given a string s and the index of a '(' in it,
    return a tuple (content, pos) where content is the substring
    inside the outer matching parentheses and pos is the index
    immediately after the matching closing ')'.

    This function takes special care to ignore any parentheses
    that occur inside quotes (a quoted literal is any part enclosed by
    ', " or `) and also to skip over escape sequences in single/double quotes.
    (Backticks do not process backslash escapes.)
    """
    if s[start_index] != '(':
        raise ValueError("Expected '(' at position {}".format(start_index))
    depth = 1
    i = start_index + 1
    content_start = i
    in_quote = None  # will be set to a quoting character when inside a quoted literal

    # Allow these quote characters.
    allowed_quotes = ("'", '"', '`')

    while i < len(s):
        c = s[i]
        if in_quote:
            # Inside a quoted literal.
            if in_quote in ("'", '"'):
                if c == '\\':
                    # Skip the escape character and the next character.
                    i += 2
                    continue
            # Whether we are in a backtick or one of the other quotes,
            # check for the closing quote.
            if c == in_quote:
                # Check for a doubled quote.
                if i + 1 < len(s) and s[i + 1] == in_quote:
                    i += 2
                    continue
                else:
                    in_quote = None
                    i += 1
                    continue
            else:
                i += 1
                continue
        else:
            # Not inside a quoted literal.
            if c in allowed_quotes:
                in_quote = c
                i += 1
                continue
            elif c == '(':
                depth += 1
                i += 1
                continue
            elif c == ')':
                depth -= 1
                i += 1
                if depth == 0:
                    # Return the substring inside (excluding the outer parentheses)
                    return s[content_start:i - 1], i
                continue
            else:
                i += 1

    raise ValueError("Unbalanced parentheses in enum definition")


def _parse_enum_values(content):
    """
    Given the inner text from an ENUM declaration—for example:
           "'point', 'qwe', 'def'"
    parse and return a list of the string values as MySQL would see them.

    This function handles:
      - For single- and double–quoted strings: backslash escapes and doubled quotes.
      - For backtick–quoted identifiers: only doubled backticks are recognized.
    """
    values = []
    i = 0
    allowed_quotes = ("'", '"', '`')
    while i < len(content):
        # Skip any whitespace.
        while i < len(content) and content[i].isspace():
            i += 1
        if i >= len(content):
            break
        # The next non–whitespace character must be one of the allowed quotes.
        if content[i] not in allowed_quotes:
            raise ValueError("Expected starting quote for enum value at position {} in {!r}".format(i, content))
        quote = content[i]
        i += 1  # skip the opening quote

        literal_chars = []
        while i < len(content):
            c = content[i]
            # For single- and double–quotes, process backslash escapes.
            if quote in ("'", '"') and c == '\\':
                if i + 1 < len(content):
                    next_char = content[i + 1]
                    # Mapping for common escapes. (For the quote character, map it to itself.)
                    escapes = {
                        '0': '\0',
                        'b': '\b',
                        'n': '\n',
                        'r': '\r',
                        't': '\t',
                        'Z': '\x1a',
                        '\\': '\\',
                        quote: quote
                    }
                    literal_chars.append(escapes.get(next_char, next_char))
                    i += 2
                    continue
                else:
                    # Trailing backslash – treat it as literal.
                    literal_chars.append('\\')
                    i += 1
                    continue
            elif c == quote:
                # Check for a doubled quote (works for all three quoting styles).
                if i + 1 < len(content) and content[i + 1] == quote:
                    literal_chars.append(quote)
                    i += 2
                    continue
                else:
                    i += 1  # skip the closing quote
                    break  # end of this literal
            else:
                # For backticks, we do not treat backslashes specially.
                literal_chars.append(c)
                i += 1
        # Finished reading one literal; join the characters.
        value = ''.join(literal_chars)
        values.append(value)

        # Skip whitespace after the literal.
        while i < len(content) and content[i].isspace():
            i += 1
        # If there's a comma, skip it; otherwise, we must be at the end.
        if i < len(content):
            if content[i] == ',':
                i += 1
            else:
                raise ValueError("Expected comma between enum values at position {} in {!r}"
                                 .format(i, content))
    return values


def is_enum_type(field_type):
    """
    Check if a field type is an enum type
    
    Args:
        field_type: The MySQL field type string
        
    Returns:
        bool: True if it's an enum type, False otherwise
    """
    return field_type.lower().startswith('enum(') 

if __name__ == '__main__':
    tests = [
        "enum('point','qwe','def')",
        "ENUM('asd', 'qwe', 'def')",
        'enum("first",  \'second\', "Don""t stop")',
        "enum('a\\'b','c\\\\d','Hello\\nWorld')",
        # Now with backticks:
        "enum(`point`,`qwe`,`def`)",
        "enum('point',`qwe`,'def')",
        "enum(`first`, `Don``t`, `third`)",
    ]

    for t in tests:
        try:
            result = parse_mysql_enum(t)
            print("Input: {}\nParsed: {}\n".format(t, result))
        except Exception as e:
            print("Error parsing {}: {}\n".format(t, e))