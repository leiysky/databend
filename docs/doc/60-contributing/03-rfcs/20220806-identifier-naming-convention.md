---
title: Identifier case sensitivity
description: RFC for case sensitivity of identifier in databend
---

- RFC PR: [todo]()
- Tracking Issue: [datafuselabs/databend#6042](https://github.com/datafuselabs/databend/issues/6042)

## Summary

In SQL query language, users can reference columns with identifier.

Different from most of the programming languages, identifiers in SQL language are always case-insensitive. [SQL specification 92](http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt) defined that, identifier can be **unquoted**(e.g. `abc`) or **quoted**(e.g. `"AbC"`), and the **unquoted** identifers are always case-insensitive.

In the real world systems, such as **PostgreSQL** and **MySQL**, have different implementation of the case-sensitivity.

In **PostgreSQL**, by default, **unquoted** identifiers are case-insensitive, which means such two identifiers `Abc` and `aBc` with same content but in different case are semanticly equivalent. In other words, a table defined as `create table t(a int)` can accept both of the queries `select a from t` and `select A from T`.

In opposite, **quoted** identifiers are case-sensitive, and a table created with **quoted** identifier(e.g. `create table "T"(a int)`) can only be accessed with **quoted** identifier in correct case.

While in **MySQL**, all of the identifiers are case-insensitive by default, whether **quoted** or **unquoted**. What's interesting is, in a period **MySQL** have different behaviors on case-sensitivity on Linux and Windows for some reason.

In databend, we don't have a formal specification for this, and it has become a issue for a long time. This RFC is aim to propose a specification of case-sensitivity of identifiers in databend, and provide a mechanism to make it possible to be compatible with different case-sensitivity dialect.

## Design

