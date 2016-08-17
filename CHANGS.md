# Changes

**0.9.3**

* DBAccess.rollingback is never null. If null is assigned, false will be stored instead.

**0.9.1**

* Support virtual columns

**0.9.0**

* Remove `DBAccess.after()`, and replaced with `DBAccess.afterCommit()` and
  `DBAccess.afterRollback()`.
* `DBAccess.rollingback` can be set with any value.
