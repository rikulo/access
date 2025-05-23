# Changes

**3.7.2**

* Improves the generation of limit 1 for `queryAnyFrom`

**3.7.1**

* The `values` paramter of `DBAccess.query` and `execute` must be `Map`.
* `DBAccess.queryBy` and `queryAnyBy` support the `shortcut` parameter.

**3.6.2**

* Fix: `DBAccess.queryFrom()` accepts an empty string for `whereClause`.

**3.6.1**

* Fix #10: assume `like()`'s pattern was encoded properly if escape is specified

**3.6.0**

* `like` and `notLike` added for generating the LIKE clause for `queryBy` and `queryAnyBy`
* `encodeTextInLike` and `encodeTextInRegex` added.
* `sqlWhereBy` supports an empty key for appending `order by` or `group by` at the end.

**3.5.0**

* `queryWith` deprecated and replaced with `queryFrom`
* `queryAnyWith` deprecated and replaced with `queryAnyFrom`
* `inList` and `notIn` added for generating the IN clause for `queryBy` and `queryAnyBy`
* `loadWhile` deprecated. Please use `await` instead.
* `Not` renamed to `NotCondition`

**3.1.1**

* `addSqlColumns` added.
* `queryWith`'s `whereClause` can have no condition, but `order`, `limit`...

**3.1.0**

* `onAccess`'s signature changed.

**3.0.1**

* The `onAccess` callback added for monitoring the transactions.

**2.6.1**

* `pgInvalidRegex`, `pgProgramLimitExceeded` and `pgOutOfMemory` added.

**2.6.0**

* The signature of `shallLogError` callback changed to `bool shallLogError(DBAccess access, String sql, Object ex)`.

**2.5.2**

* `onQuery` and `onExecute` of `configure()` are deprecated. Please configure [Pool](https://pub.dev/documentation/postgresql2/latest/postgresql.pool/Pool-class.html) instead.

**2.5.1**

* `DBAccess.exists` added

**2.5.0**

* Use `AccessOption` introduced in entity 2.5.0

**2.0.2**

* `DBAccess.toEntityNS` added

**2.0.0**

* Migrate to null safety

**1.6.4**

* `onQuery` and `onExecute` supported in `configure` for easy debugging.

**1.6.2**

* `accessCount` introduced for knowing number of accesses being executed.

**1.6.0**

* The signature of `onSlowSql` changed. An extra argument called `values` added.

**1.5.0**

* `DBAccess.begin()` introduced for users to control transactions explicitly.

**1.3.1**

* `DBAccess.isRollingback` introduced.

**1.3.0**

* `configure()`'s `onPreSlowSql` argument is enhanced. You can store the message in `onPreSlowSql` and retrieve it back in `onSlowSql`.

**1.2.1**

* `access()`'s `command` argument can return `FutureOr<T>`

**1.2.0**

* `DBAccess.afterCommit()` and `afterRollback()` will execute the given task even if the connection was closed -- depending it was committed or rolled back.
Also, the task can return a `Future` instance.

**1.1.1**

* `DBAccess.close()` introduced to allow user to force a transaction to close earlier.

**1.1.0**

* `DBAccess.tag` and `configure()`'s `onTag` no longer supported.
* `onSlowSql` and `onPreSlowSql`'s signatures changed.
* `configure()`'s `slowSql` renamed to `slowSqlThreshold`.

**1.0.6**

* `onPreSlowSql` introduced to log the information about locks when detecting a slow SQL statement.

**1.0.5**

* `primaryKey` introduced to define a table with multi-column primary key.

**1.0.4**

* `not()` introduced to specify a negative condition in the `whereValues` condition.

**1.0.2**

* `Index()` supports `where` for creating a partial index.

**0.11.3**

* API of sqlWhereBy is changed -- no option argument

**0.11.1**

* Adds the `otype` argument to `UnboundReference()` for documentation purpose

**0.11.0**

* Use named paramters instead of positional parameters for declaring types
* The column that `Reference()` references can be specified.

**0.10.1**

* `afterComment()` and `afterRollback()` can return an optional Future instance

**0.10.0**

* `Citext` added
* The signature of the create method is changed.

**0.9.10**

* Slow SQL warning logs the previous SQL statement instead if it is `commit`

**0.9.8**

* The signature of the shallLogError argument has been changed.

**0.9.7**

* Apply the generic method syntax (so it requires Dart 1.21 or later)

**0.9.3**

* DBAccess.rollingback is never null. If null is assigned, false will be stored instead.

**0.9.1**

* Support virtual columns

**0.9.0**

* Remove `DBAccess.after()`, and replaced with `DBAccess.afterCommit()` and
  `DBAccess.afterRollback()`.
* `DBAccess.rollingback` can be set with any value.
