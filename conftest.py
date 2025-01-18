# conftest.py
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-optional",
        action="store_true",
        default=False,
        help="Run tests marked as optional",
    )


def pytest_collection_modifyitems(config, items):
    run_optional = config.getoption("--run-optional")
    keyword = config.getoption("keyword")  # Retrieves the value passed with -k

    selected_tests = set()

    if keyword:
        # Collect nodeids of tests that match the -k keyword expression
        for item in items:
            if keyword in item.name or keyword in item.nodeid:
                selected_tests.add(item.nodeid)

    for item in items:
        if "optional" in item.keywords:
            if run_optional or item.nodeid in selected_tests:
                # Do not skip if --run-optional is set or if the test matches the -k expression
                continue
            else:
                # Skip the test
                skip_marker = pytest.mark.skip(reason="Optional test, use --run-optional to include")
                item.add_marker(skip_marker)
