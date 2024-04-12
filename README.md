# tailsql

tailsql can continually run sql query on tumbling/sliding window of log files records.

# why

- when reading logs using `tail -f`, sometimes it flushs too fast to read
- sometime you just wanna do some simple aggregation/join analytics, and awk can't express this with one line
- you wanna aggregate data like stream job, but traditional unix tools works like batch job
- you wanna compare two or more log files on sepecific fields but other records rushed into the small screen

# usage

It's a go-stream pipeline job like:

```
log -> parse -> filter -> throttler -> sql row \
log -> parse -> filter -> throttler -> sql row -> window -> insert as table -> run sql(duckdb/sqlite) -> sink
log -> parse -> filter -> throttler -> sql row /
```

for example log format like this: `2024/04/12 22:47:42.506277 GETM SUCC localhost:7710 605` (ts method result server time_used)

you can run query to analytic your log

```bash
tailsql query \
    -f my.log \
    # use capture group to parse log to table row
    # __ is seperator before is field name after is data type
    # now support int/float/date/str
    -r '.+ (?P<method__str>GETM) (?P<status__str>SUCC) .+7710 (?P<time__int>[0-9]+) .+' \
    # only contain time > 10000 records
    # filter syntax is like SQL where
    -F 'time > 10000' \
    -s stdout \
    # sliding windows, size 10s sliding interval 5s, use input time not log time
    # you can use your logs time, just change -1 to the index of your ts capture group
    -w '10:5:-1' \
    # parsed rows filtered then insert to db engine for anylytic
    # support duckdb/sqlite/qlbridge membtree
    -d duckdb \
    # format as table
    -o table \
    # query
    'select count(1) from t0 where time > 12275'
```

will get result like:

```
>>> Press CTRL + C to quit ...
INFO[2024-04-12T22:41:56+08:00] Query stream started, please wait 10s...
INFO[2024-04-12T22:42:06+08:00] >> query result <<
Run sql `select count(1) from t0 where time > 12275`:
+----------+
| COUNT(1) |
+----------+
| 71       |
+----------+
INFO[2024-04-12T22:42:11+08:00] >> query result <<
Run sql `select count(1) from t0 where time > 12275`:
+----------+
| COUNT(1) |
+----------+
| 86       |
+----------+
INFO[2024-04-12T22:42:16+08:00] >> query result <<
Run sql `select count(1) from t0 where time > 12275`:
+----------+
| COUNT(1) |
+----------+
| 81       |
+----------+
INFO[2024-04-12T22:42:21+08:00] >> query result <<
Run sql `select count(1) from t0 where time > 12275`:
+----------+
| COUNT(1) |
+----------+
| 63       |
+----------+
^C>>> User ask to quit ...
```
