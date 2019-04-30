# Changes

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
