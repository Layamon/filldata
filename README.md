# filldata

```
create table my_tbl
(
  c1 int,
  c2 int,
  c3 text,
  c4 varchar(16),
  c5 json,
  c6 bool,
  c7 float
);
```

time ./target/release/filldata --user liuyangming --dbname tdb --table my_tbl --port 8100 --rows 1000000

