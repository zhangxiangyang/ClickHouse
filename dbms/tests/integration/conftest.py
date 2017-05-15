import difflib

from helpers.client import TSV


def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, TSV) and isinstance(right, TSV) and op == '==':
        diff = list(difflib.context_diff(left.lines, right.lines))
        return ['TabSeparated values differ: '] + [line.rstrip() for line in diff[2:]]
