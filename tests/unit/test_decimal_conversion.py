#!/usr/bin/env python3
"""
Unit tests to verify decimal type conversion fix
"""

from mysql_ch_replicator.converter import MysqlToClickhouseConverter


def test_decimal_conversions():
    """Test various decimal type conversions"""
    converter = MysqlToClickhouseConverter()

    test_cases = [
        # (mysql_type, parameters, expected_result)
        ("decimal(14,4)", "", "Decimal(14, 4)"),
        ("decimal(10,2)", "", "Decimal(10, 2)"),
        ("decimal(18,0)", "", "Decimal(18, 0)"),
        ("decimal(5)", "", "Decimal(5, 0)"),
        ("decimal", "", "Decimal(10, 0)"),
        ("DECIMAL(14,4)", "", "Decimal(14, 4)"),  # Test case insensitive
        ("decimal(10, 2)", "", "Decimal(10, 2)"),  # Test with spaces
    ]

    print("Testing decimal type conversions:")
    print("=" * 50)

    for mysql_type, parameters, expected in test_cases:
        result = converter.convert_type(mysql_type, parameters)
        assert result == expected, (
            f"Failed for {mysql_type}: got {result}, expected {expected}"
        )
        print(f"✓ PASS: {mysql_type:<15} -> {result}")

    print("=" * 50)
    print("🎉 All tests passed!")


def test_nullable_decimal():
    """Test decimal with nullable parameters"""
    converter = MysqlToClickhouseConverter()

    test_cases = [
        # (mysql_type, parameters, expected_result)
        ("decimal(14,4)", "null", "Nullable(Decimal(14, 4))"),
        ("decimal(10,2)", "not null", "Decimal(10, 2)"),
        ("decimal(18,6)", "default 0.000000", "Nullable(Decimal(18, 6))"),
    ]

    print("\nTesting decimal field type conversions (with nullability):")
    print("=" * 50)

    for mysql_type, parameters, expected in test_cases:
        result = converter.convert_field_type(mysql_type, parameters)
        assert result == expected, (
            f"Failed for {mysql_type} ({parameters}): got {result}, expected {expected}"
        )
        print(f"✓ PASS: {mysql_type} ({parameters}) -> {result}")

    print("=" * 50)
    print("🎉 All nullable tests passed!")


def test_decimal_conversion_comprehensive():
    """Comprehensive test using pytest assertions for CI/CD"""
    converter = MysqlToClickhouseConverter()

    # Test basic decimal conversions
    assert converter.convert_type("decimal(14,4)", "") == "Decimal(14, 4)"
    assert converter.convert_type("decimal(10,2)", "") == "Decimal(10, 2)"
    assert converter.convert_type("decimal(18,0)", "") == "Decimal(18, 0)"
    assert converter.convert_type("decimal(5)", "") == "Decimal(5, 0)"
    assert converter.convert_type("decimal", "") == "Decimal(10, 0)"

    # Test case insensitive
    assert converter.convert_type("DECIMAL(14,4)", "") == "Decimal(14, 4)"

    # Test with spaces
    assert converter.convert_type("decimal(10, 2)", "") == "Decimal(10, 2)"

    # Test nullable conversions
    assert (
        converter.convert_field_type("decimal(14,4)", "null")
        == "Nullable(Decimal(14, 4))"
    )
    assert converter.convert_field_type("decimal(10,2)", "not null") == "Decimal(10, 2)"
    assert (
        converter.convert_field_type("decimal(18,6)", "default 0.000000")
        == "Nullable(Decimal(18, 6))"
    )


if __name__ == "__main__":
    print("Running decimal conversion tests...")

    try:
        test_decimal_conversions()
        test_nullable_decimal()
        test_decimal_conversion_comprehensive()

        print(f"\n{'=' * 50}")
        print("🎉 ALL TESTS PASSED! Decimal conversion fix is working correctly.")
    except AssertionError as e:
        print(f"\n{'=' * 50}")
        print(f"❌ TEST FAILED: {e}")
    except Exception as e:
        print(f"\n{'=' * 50}")
        print(f"❌ ERROR: {e}")
