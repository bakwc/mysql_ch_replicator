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


def test_decimal_14_4_not_float64():
    """
    Specific test to verify that decimal(14,4) converts to Decimal(14, 4) and NOT Float64
    This addresses the issue mentioned where decimal was incorrectly converted to Float64
    """
    converter = MysqlToClickhouseConverter()

    # Test the specific case mentioned in the issue
    result = converter.convert_type("decimal(14,4)", "")

    # Assert it converts to Decimal, not Float64
    assert result == "Decimal(14, 4)", (
        f"decimal(14,4) incorrectly converts to {result}, expected Decimal(14, 4)"
    )
    assert result != "Float64", (
        f"decimal(14,4) should NOT convert to Float64, got {result}"
    )

    # Test field type conversion as well
    field_result = converter.convert_field_type("decimal(14,4)", "")
    assert field_result == "Nullable(Decimal(14, 4))", (
        f"decimal(14,4) field incorrectly converts to {field_result}"
    )
    assert "Float64" not in field_result, (
        f"decimal(14,4) field should NOT contain Float64, got {field_result}"
    )

    # Test not null version
    not_null_result = converter.convert_field_type("decimal(14,4)", "not null")
    assert not_null_result == "Decimal(14, 4)", (
        f"decimal(14,4) not null incorrectly converts to {not_null_result}"
    )
    assert "Float64" not in not_null_result, (
        f"decimal(14,4) not null should NOT contain Float64, got {not_null_result}"
    )


def test_decimal_vs_float_types():
    """Test to ensure decimal types are clearly distinguished from float types"""
    converter = MysqlToClickhouseConverter()

    # Test that decimal types convert to Decimal
    decimal_cases = [
        ("decimal(14,4)", "Decimal(14, 4)"),
        ("decimal(10,2)", "Decimal(10, 2)"),
        ("decimal(18,6)", "Decimal(18, 6)"),
        ("DECIMAL(5,2)", "Decimal(5, 2)"),
    ]

    for mysql_type, expected in decimal_cases:
        result = converter.convert_type(mysql_type, "")
        assert result == expected, (
            f"{mysql_type} should convert to {expected}, got {result}"
        )
        assert "Float" not in result, (
            f"{mysql_type} should NOT contain Float, got {result}"
        )

    # Test that float types convert to Float (not Decimal)
    float_cases = [
        ("float", "Float32"),
        ("double", "Float64"),
        ("real", "Float64"),
    ]

    for mysql_type, expected in float_cases:
        result = converter.convert_type(mysql_type, "")
        assert result == expected, (
            f"{mysql_type} should convert to {expected}, got {result}"
        )
        assert "Decimal" not in result, (
            f"{mysql_type} should NOT contain Decimal, got {result}"
        )


if __name__ == "__main__":
    print("Running decimal conversion tests...")

    try:
        test_decimal_conversions()
        test_nullable_decimal()
        test_decimal_conversion_comprehensive()
        test_decimal_14_4_not_float64()
        test_decimal_vs_float_types()

        print(f"\n{'=' * 50}")
        print("🎉 ALL TESTS PASSED! Decimal conversion fix is working correctly.")
        print(
            "✅ Verified: decimal(14,4) correctly converts to Decimal(14, 4), NOT Float64"
        )
    except AssertionError as e:
        print(f"\n{'=' * 50}")
        print(f"❌ TEST FAILED: {e}")
    except Exception as e:
        print(f"\n{'=' * 50}")
        print(f"❌ ERROR: {e}")
