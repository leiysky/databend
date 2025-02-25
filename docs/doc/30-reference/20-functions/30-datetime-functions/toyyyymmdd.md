---
title: toYYYYMMDD
---

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY * 10000 + MM * 100 + DD).
## Syntax

```sql
toYYYYMMDD(expr)
```

## Return Type

UInt32, returns in `YYYYMMDD` format.

## Examples

```sql
SELECT to_date(18875);
+---------------+
| to_date(18875) |
+---------------+
| 2021-09-05    |
+---------------+

SELECT toYYYYMMDD(to_date(18875));
+---------------------------+
| toYYYYMMDD(to_date(18875)) |
+---------------------------+
|                  20210905 |
+---------------------------+

SELECT to_datetime(1630833797);
+------------------------+
| to_datetime(1630833797) |
+------------------------+
| 2021-09-05 09:23:17    |
+------------------------+

SELECT toYYYYMMDD(to_datetime(1630833797));
+------------------------------------+
| toYYYYMMDD(to_datetime(1630833797)) |
+------------------------------------+
|                           20210905 |
+------------------------------------+
```
