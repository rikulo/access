//Copyright (C) 2021 Potix Corporation. All Rights Reserved.
//History: Tue Apr 27 12:47:15 CST 2021
// Author: tomyeh
part of access;

/** Configures the access library.
 * 
 * Note: it must be called with a non-null pool before calling [access]
 * to start a transaction.
 * 
 * * [pool] - the pool used to establish a connection
 * * [slowSql] - how long to consider a query or an execution is slow.
 * It is used to detect if any slow SQL statement. Default: null (no detect).
 * * [onSlowSql] - if specified, it is called when a slow query is detected.
 * The `dataset` argument will be [DBAccess.dataset], so you can use it to
 * pass information to this callback.
 * If not specified, the slow SQL statement will be logged directly.
 * * [onPreSlowSql] = if specified, it is called right before [onSlowSql].
 * And, the `message` argument will carry the information about locks.
 * The implementation can use the `conn` argument to retrieve more from
 * the database. It is a different transaction than the one causing
 * slow SQL. If not specified, nothing happens.
 * The `dataset` argument will be [DBAccess.dataset], so you can use it to
 * store the message, and then retrieve it in [onSlowSql].
 * * [onQuery] - a callback when [DBAccess.query] is called.
 * It is used for debugging purpose.
 * * [onExecute] - a callback when [DBAccess.execute] is called.
 * It is used for debugging purpose.
 * * [getErrorMessage] - if specified, it is called to retrieve
 * a human readable message of the given [sql] and [values] when an error occurs.
 * Default: it returns a string concatenating [sql] and [values].
 * * [shallLogError] - test if the given exception shall be logged.
 * Default: always true. You can turn the log off by returning false.
 * 
 * * It returns the previous pool, if any.
 */
Pool configure(Pool pool, {Duration? slowSqlThreshold,
    void onSlowSql(Map<String, dynamic> dataset, Duration timeSpent, String sql, dynamic values)?,
    FutureOr onPreSlowSql(Connection conn, Map<String, dynamic> dataset, String message)?,
    void onQuery(String sql, dynamic values)?,
    void onExecute(String sql, dynamic values)?,
    String getErrorMessage(String sql, dynamic values)?,
    bool shallLogError(DBAccess access, ex)?}) {
  final p = _pool;
  _pool = pool;
  _defaultPreSlowSqlThreshold = _calcPreSlowSql(
      _defaultSlowSqlThreshold = slowSqlThreshold);
  _onSlowSql = onSlowSql ?? _defaultOnSlowSql;
  _onPreSlowSql = onPreSlowSql;
  _onQuery = onQuery;
  _onExecute = onExecute;
  _getErrorMessage = getErrorMessage ?? _defaultErrorMessage;
  _shallLogError = shallLogError ?? _defaultShallLog;
  return p;
}
late Pool _pool;
///How long to consider an execution slow
Duration? _defaultSlowSqlThreshold,
///How long to log locking and other info (95% of [_defaultSlowSqlThreshold])
  _defaultPreSlowSqlThreshold;

Duration? _calcPreSlowSql(Duration? dur)
=> dur == null ? null: Duration(microseconds: (dur.inMicroseconds * 95) ~/ 100);

late void Function(Map<String, dynamic> dataset, Duration timeSpent, String sql, dynamic values)
  _onSlowSql;
FutureOr Function(Connection conn, Map<String, dynamic> dataset, String message)?
  _onPreSlowSql;
void Function(String sql, dynamic values)? _onQuery, _onExecute;
late String Function(String sql, dynamic values) _getErrorMessage;
String _defaultErrorMessage(String sql, dynamic values) => sql;

void _defaultOnSlowSql(Map<String, dynamic> dataset, Duration timeSpent,
    String sql, var values) {
  _logger.warning("Slow SQL ($timeSpent): $sql");
}

late bool Function(DBAccess access, Object ex) _shallLogError;
bool _defaultShallLog(DBAccess access, ex) => true;
