# Changes

**0.10.0**

* Citext added
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
