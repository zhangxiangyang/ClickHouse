### ClickHouse integration tests

Tests are invoked via [py.test](https://docs.pytest.org/). Install with: `sudo pip install pytest`.
Run with `pytest`.

To add new test, create a directory `test_foo` with a file `run.py` in it. Functions starting with `test` will become test cases. File `helpers.py` contains helpers to launch multiple ClickHouse instances via docker.
