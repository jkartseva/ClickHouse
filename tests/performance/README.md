## ClickHouse performance tests

This directory contains `.xml`-files with performance tests for @akuzm tool.

### How to write performance test

First of all you should check existing tests don't cover your case. If there are no such tests then you should write your own.

You can use `substitions`, `create`, `fill` and `drop` queries to prepare test. You can find examples in this folder.

If your test takes more than 10 minutes, please, add tag `long` to have an opportunity to run all tests and skip long ones.

### How to run performance test

TODO @akuzm

### How to validate single test

```
pip3 install clickhouse_driver scipy
../../tests/performance/scripts/perf.py --runs 1 insert_parallel.xml
```
