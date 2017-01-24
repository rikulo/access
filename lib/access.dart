//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Thu, Jul 03, 2014  4:43:13 PM
// Author: tomyeh
library access;

import "dart:async";
import "dart:collection" show HashSet, HashMap;

import "package:logging/logging.dart" show Logger;
import "package:postgresql2/postgresql.dart";
import "package:postgresql2/pool.dart";
import "package:entity/postgresql2.dart";
import "package:entity/entity.dart";
import "package:rikulo_commons/util.dart";

export "package:postgresql2/postgresql.dart"
  show Connection, PostgresqlException, Row;

final Logger _logger = new Logger("access");

const String
  PG_SUCCESSFUL_COMPLETION = "00000",
  PG_WARNING = "010000",
  PG_NO_DATA = "020000",
  PG_DUPLICATE_TABLE = "42P07",
  PG_FAILED_IN_TRANSACTION = "25P02",
  PG_UNDEFINED_OBJECT = "42704",
  PG_UNDEFINED_TABLE = "42P01",
  PG_INTEGRITY_CONSTRAINT_VIOLATION = "23000",
  PG_NOT_NULL_VIOLATION =  "23502",
  PG_FOREIGN_KEY_VIOLATION = "23503",
  PG_UNIQUE_VIOLATION = "23505",
  PG_CHECK_VIOLATION = "23514";

///Whether it is [PostgresqlException] about the violation of the given [code].
bool isViolation(ex, String code)
=> ex is PostgresqlException && ex.serverMessage != null
    && ex.serverMessage.code == code;

///Whether it is [PostgresqlException] about the violation of uniqueness.
///It is useful with select-for-update
bool isUniqueViolation(ex) => isViolation(ex, PG_UNIQUE_VIOLATION);
///Whether it is [PostgresqlException] about the violation of foreign keys.
bool isForeignKeyViolation(ex) => isViolation(ex, PG_FOREIGN_KEY_VIOLATION);
///Whether it is [PostgresqlException] about the violation of foreign keys.
bool isNotNullViolation(ex) => isViolation(ex, PG_NOT_NULL_VIOLATION);

/** Executes a command within a transaction.
 * 
 *    access((DBAccess access) async {
 *      await for (final Row row in await access.query('select ...')) {
 *        ...
 *      }
 *      ...
 *      await access.execute('update...');
 *    })
 *    //The transaction ends here. It commits if success,
 *    //Or, rolls back if an exception is thrown or [DBAccess.rollingback]
 *    //is set to a value other than false and null.
 *    .catchError((ex, st) {
 *      ...
 *    });
 *
 * It returns what was returned by [command].
 */
Future<T> access<T>(Future<T> command(DBAccess access)) async {
  var error;
  bool closing = false;
  DBAccess access;

  try {
    access = new DBAccess._(await _pool.connect());
    await access._begin();

    final result = await command(access);

    closing = true;
    if (access.rollingback == false) {
      await access._commit();
    } else {
      error = access.rollingback; //yes, use it as an error
      await access._rollback();
    }

    return result;

  } catch (ex) {
    error = ex;
    if (access != null && !closing)
      await access._rollback()
      .catchError(_rollbackError);

    rethrow;

  } finally {
    access?._close(error);
  }
}

void _rollbackError(ex, st)
=> _logger.warning("Failed to rollback", ex, st);
_asNull(_) => null;
bool _isStateError(ex) => ex is StateError;

typedef void _ErrorTask(error);
typedef void _Task();

/** The database access.
 * It is designed to used with [access].
 */
class DBAccess extends PostgresqlAccess {
  Map<String, dynamic> _dataset;
  List<_Task> _afterCommits;
  List<_ErrorTask> _afterRollbacks;
  bool _closed = false;

  /// Whether the connect is closed.
  bool get closed => _closed;

  /**
   * A flag or a cause to indicate the access (aka., the transaction) shall
   * be rolled back at the end.
   * 
   * By default, [access] rolls back
   * only if an exception is thrown. To force it roll back, you
   * can set this flag to true or a value other than false and null.
   * 
   * Note: [access] will still return the return value of the command function
   * if a flag is set.
   * 
   * Note: if a value other than false and null is set,
   * the callback passed to [afterRollback] will be called with this value.
   * 
   * Note: if null is assigned, *false* will be stored instead.
   * In other words, it is never null. You can test if rollingback is set
   * as follows:
   * 
   *     if (access.rollingback != false)...
   */
  get rollingback => _rollingback;
  /** Sets whether to roll back the access (aka., the transaction).
   * 
   * Note: if null is assigned, *false* will be stored instead.
   */
  set rollingback(rollingback) {
    _rollingback = rollingback ?? false;
  }
  var _rollingback = false;

  DBAccess._(Connection conn): super(conn, cache: true);

  /** How long to consider the query or execution of a SQL statement is slow.
   * If omitted, the value specified in [configure] is used.
   */
  Duration slowSql;
  Duration get _realSlowSql => slowSql ?? _slowSql;

  /** A map of application-specific data.
   */
  Map<String, dynamic> get dataset
  => _dataset != null ? _dataset: MapUtil.auto(() => _dataset = new HashMap());

  ///Tags the next SQL statement in this access (aka., transaction).
  ///Once tagged, [onTag] will be called in the next invocation
  ///of [query] or [execute]. If [onTag] is not specified, the SQL statement
  ///is simply logged.
  var tag;

  /** Adds a task that will be executed after the transaction is committed
   * successfully
   */
  void afterCommit(void task()) {
    assert(task != null);
    if (_closed)
      throw new StateError("Closed");

    if (_afterCommits == null)
      _afterCommits = [];
    _afterCommits.add(task);
  }
  /** Adds a task that will be executed after the transaction is committed
   * successfully
   */
  void afterRollback(void task(error)) {
    assert(task != null);
    if (_closed)
      throw new StateError("Closed");

    if (_afterRollbacks == null)
      _afterRollbacks = [];
    _afterRollbacks.add(task);
  }

  void _close(error) {
    _closed = true;
    try {
      conn.close();
    } catch (ex, st) {
      _logger.warning("Failed to close", ex, st);
    }

    if (error != null) {
      if (_afterRollbacks != null)
        for (final _ErrorTask task in _afterRollbacks) {
          try {
            task(error);
          } catch (ex, st) {
            _logger.warning("Failed to invoke $task with $error", ex, st);
          }
        }
    } else {
      if (_afterCommits != null)
        for (final _Task task in _afterCommits) {
          try {
            task();
          } catch (ex, st) {
            _logger.warning("Failed to invoke $task", ex, st);
          }
        }
    }
  }

  /// Queues a command for execution, and when done, returns the number of rows
  /// affected by the SQL command.
  @override
  Future<int> execute(String sql, [values]) async {
    if (_closed)
      throw new StateError("Closed: ${_getErrorMessage(sql, values)}");

    _checkTag(sql, values);

    try {
      if (_realSlowSql != null) {
        final DateTime started = new DateTime.now();
        final result = await conn.execute(sql, values);
        _checkSlowSql(started, sql, values);
        return result;
      } else {
        return conn.execute(sql, values);
      }

    } catch (ex, st) {
      if (_shallLogError(this, ex))
        _logger.severe("Failed to execute: ${_getErrorMessage(sql, values)}", ex, st);
      rethrow;
    }
  }

  /// Queue a SQL query to be run, returning a [Stream] of rows.
  @override
  Stream<Row> query(String sql, [values]) {
    if (_closed)
      throw new StateError("Closed: ${_getErrorMessage(sql, values)}");

    _checkTag(sql, values);

    final StreamController controller = new StreamController();
    final DateTime started = _realSlowSql != null ? new DateTime.now(): null;
    conn.query(sql, values)
      .listen((Row data) => controller.add(data),
        onError: (ex, st) {
          if (_shallLogError(this, ex))
            _logger.severe("Failed to query: ${_getErrorMessage(sql, values)}", ex, st);
          controller.addError(ex, st);
        },
        onDone: () {
          controller.close();

          if (started != null)
            _checkSlowSql(started, sql, values);
        },
        cancelOnError: true);
    return controller.stream;
  }

  ///Check if it is tagged.
  void _checkTag(String sql, [values]) {
    if (tag != null) {
      try {
        if (_onTag != null)
          _onTag(this, tag, sql, values);
        else
          _logger.warning('[$tag] SQL: ${_defaultErrorMessage(sql, values)}');
      } finally {
        tag = null; //do it only once
      }
    }
  }
  ///Checks if it is slow. If so, logs it.
  void _checkSlowSql(DateTime started, String sql, [values]) {
    final Duration spent = new DateTime.now().difference(started);
    final Duration threshold = _realSlowSql;
    if (threshold != null && spent > threshold) {
      if (_onSlowSql != null) {
        _onSlowSql(this, spent, sql, values);
      } else {
        tag = "after slow";
        _logger.warning('Slow SQL ($spent): ${_getErrorMessage(sql, values)}');
      }
    }
  }

  ///Returns the first result, or null if not found.
  Future<Row> queryAny(String sql, [values])
  => query(sql, values).first
    .catchError(_asNull, test: _isStateError);

  /** Queries [fields] of [otype] for the criteria specified in
   * [whereValues] (AND-ed together).
   * 
   * * [option] - whether to use [FOR_UPDATE], [FOR_SHARE] or null.
   */
  Stream<Row> queryBy(Iterable<String> fields, String otype,
    Map<String, dynamic> whereValues, [int option])
  => _queryBy(fields, otype, whereValues, option, null);

  Stream<Row> _queryBy(Iterable<String> fields, String otype,
    Map<String, dynamic> whereValues, int option, String append)
  => queryWith(fields, otype,
      sqlWhereBy(whereValues, option, append), whereValues);

  /** Queries [fields] of [otype] for the criteria specified in
   * [whereValues] (AND-ed together), or null if not found.
   */
  Future<Row> queryAnyBy(Iterable<String> fields, String otype,
    Map<String, dynamic> whereValues, [int option])
  => _queryBy(fields, otype, whereValues, option, "limit 1").first
    .catchError(_asNull, test: _isStateError);

  /** Queries [fields] of [otype] for the criteria specified in
   * [whereClause] and [whereValues].
   * 
   * > If you'd like *select-for-update*, you can put `for update`
   * or `for share` to [whereClause].
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$F_REMOVED_AT" is not null`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$OT_TASK" inner join "$OT_ASSIGNEE"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   * 
   * Note: if [fromClause] is specified, [otype] is ignored.
   */
  Stream<Row> queryWith(Iterable<String> fields, String otype,
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut]) {
    String sql = 'select ${sqlColumns(fields, shortcut)} from ';
    sql += fromClause != null ? fromClause:
        shortcut != null ? '"$otype" $shortcut': '"$otype"';
    if (whereClause != null)
      sql += ' where $whereClause';
    return query(sql, whereValues);
  }

  /** Returns the first result, or null if not found.
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$F_REMOVED_AT" is not null`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$OT_TASK" inner join "$OT_ASSIGNEE"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<Row> queryAnyWith(Iterable<String> fields, String otype,
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut])
  => queryWith(fields, otype, whereClause, whereValues, fromClause, shortcut)
    .first
    .catchError(_asNull, test: _isStateError);

  ///Loads the entity by the given [oid], or null if not found.
  Future<Entity> load(
      Iterable<String> fields, Entity newInstance(String oid), String oid,
      [int option])
  => loadIfAny(this, oid, newInstance, fields, option);

  /** Loads all entities of the given criteria (never null).
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$F_REMOVED_AT" is not null`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$OT_TASK" inner join "$OT_ASSIGNEE"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<List<Entity>> loadAllWith(
      Iterable<String> fields, Entity newInstance(String oid),
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut]) async {
    Set<String> fds;
    if (fields != null) {
      fds = new HashSet();
      fds..add(F_OID)..addAll(fields);
    }

    final List<Entity> entities = [];
    await for (final row in
        queryWith(fds, fromClause != null ? null: newInstance('*').otype,
        whereClause, whereValues, fromClause, shortcut)) {
      entities.add(await toEntity(row, fields, newInstance));
    }
    return entities;
  }

  /** Instantiates an Entity instance to represent the data in [row].
   * If [row] is, this method will return `new Future.value(null)`.
   */
  Future<Entity> toEntity(Row row, Iterable<String> fields,
      Entity newInstance(String oid)) {
    if (row == null)
      return new Future.value();

    final Map<String, dynamic> data = new HashMap();
    row.forEach((String name, value) => data[name] = value);
    assert(data.containsKey(F_OID)); //F_OID is required.
    return loadIfAny_(this, data.remove(F_OID), newInstance,
        (Entity e, Set<String> fds, bool fu) => new Future.value(data),
        fields);
  }

  /** Loads entities while [test] returns true.
   * It stops loading if all entities are loaded or [test] returns false.
   * 
   * Note: the last entity passed to [test] will be in the returned list
   * unless you remove it in [test]. (that is, `do ... while(test())`)
   * 
   * * [test] - it is called to test if the loading shall continue (true).
   * When [test] is called, `lastLoaded` is the entity to test, and `loaded`
   * is a list of all loaded entities, *including* `lastLoaded`
   * (at the end of `loaded`).
   * Though rare, you can modify `loaded` in [test], such as removing
   * `lastLoaded` from `loaded`.
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$F_REMOVED_AT" is not null`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$OT_TASK" inner join "$OT_ASSIGNEE"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<List<Entity>> loadWhile(
      Iterable<String> fields, Entity newInstance(String oid),
      bool test(Entity lastLoaded, List<Entity> loaded),
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut]) async {

    final List<Entity> loaded = [];

    await for (final Row row in queryWith(
        fields != null ? (new HashSet.from(fields)..add(F_OID)): null,
        fromClause != null ? null: newInstance('*').otype,
        whereClause, whereValues, fromClause, shortcut)) {

      final Entity e = await toEntity(row, fields, newInstance);
      loaded.add(e); //always add (i.e., add before test)
      if (!test(e, loaded))
        break;
    }

    return loaded;
  }

  /** Loads the first entity of the given criteria, or returns null if none.
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$F_REMOVED_AT" is not null`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$OT_TASK" inner join "$OT_ASSIGNEE"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<Entity> loadWith(
      Iterable<String> fields, Entity newInstance(String oid),
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut]) async {
    Set<String> fds;
    if (fields != null) {
      fds = new HashSet();
      fds..add(F_OID)..addAll(fields);
    }

    Row row;
    try {
      row = await queryWith(fds,
        fromClause != null ? null: newInstance('*').otype,
        whereClause, whereValues, fromClause, shortcut).first;
    } on StateError catch (_) {
      //ignore
    }

    return toEntity(row, fields, newInstance);
  }

  /** Loads all entities of the given AND criteria.
   * By AND, we mean it satisfies all values in [whereValues].
   * 
   * * [option] - whether to use [FOR_SHARE], [FOR_UPDATE]
   * or null (default; no lock).
   */
  Future<List<Entity>> loadAllBy(
      Iterable<String> fields, Entity newInstance(String oid),
      Map<String, dynamic> whereValues, [int option])
  => loadAllWith(fields, newInstance,
      sqlWhereBy(whereValues, option), whereValues);

  /** Loads the first entity of the given AND criteria.
   * By AND, we mean it satisfies all values in [whereValues].
   * 
   * * [option] - whether to use [FOR_SHARE], [FOR_UPDATE]
   * or null (default; no lock).
   */
  Future<Entity> loadBy(
      Iterable<String> fields, Entity newInstance(String oid),
      Map<String, dynamic> whereValues, [int option])
  => loadWith(fields, newInstance,
      sqlWhereBy(whereValues, option, "limit 1"), whereValues);

  ///Deletes the entity of the given [oid].
  Future<int> delete(String otype, String oid) {
    uncache(otype, oid);
    return execute('delete from "$otype" where "$F_OID"=@$F_OID',
      {F_OID: oid});
  }
  /** Inserts the entity specified in data.
   * Note: all fields found in [data] are written. You have to
   * remove unnecessary files by yourself, such as [F_OTYPE].
   * 
   * * [types] - a map of (field-name, field-type). If specified,
   * the type of the field will be retrieved from [types], if any.
   * * [append] - the extra clause to append to the insert statement.
   * Example, `final oid = await insert(..., append: returning "$F_OID");`
   */
  Future<dynamic> insert(String otype, Map<String, dynamic> data,
      {Map<String, String> types, String append}) {
    final StringBuffer sql = new  StringBuffer('insert into "')
      ..write(otype)..write('"(');
    final StringBuffer param = new StringBuffer(" values(");

    bool first = true;
    for (final String fd in data.keys) {
      if (first) first = false;
      else {
        sql.write(',');
        param.write(',');
      }
      sql..write('"')..write(fd)..write('"');

      param..write('@')..write(fd);
      if (types != null) {
        final String type = types[fd];
        if (type != null)
          param..write(':')..write(type);
      }
    }

    sql.write(')');
    param.write(')');
    bool bReturning = false;
    if (append != null) {
      bReturning = append.trim().startsWith('returning');
      param..write(' ')..write(append);
    }

    sql.write(param);
    final String stmt = sql.toString();
    if (bReturning)
      return query(stmt, data).first.then(_firstCol);

    return execute(stmt, data);
  }
  static _firstCol(Row row) => row[0];

  //Begins a transaction
  Future _begin() => execute('begin');
  //Commits
  Future _commit() => execute('commit');
  //Rollback
  Future _rollback() => execute('rollback');

  @override
  String toString() => "DBAccess:$hashCode";
}

///Collects the first column of [Row] into a list.
List firstColumns(Iterable<Row> rows) {
  final List<String> result = [];
  for (final Row row in rows)
    result.add(row[0]);
  return result;
}

/** Converts a list of [fields] to a SQL fragment separated by comma.
 * 
 * Note: if [fields] is null, `"*"` is returned, i.e., all fields are assumed.
 * if [fields] is empty, `1` is returned (so it is easier to construct 
 * a SQL statement).
 * 
 * Note: if a field starts with '(', or a number, we don't encode it
 * with a double quotation. It is used to retrieve a constant, an expression,
 * or anything you prefer not to encode.
 * 
 * For example, you can pass a field as
 * `("assignee" is not null or "due" is null)`.
 * Furthermore, you can name it (aka., virtual column or calculated column):
 *  `("assignee" is not null or "due" is null) alive`
 * 
 * Here is an example of use:
 * 
 *     access.query('select ${sqlColumns(fields)} from "Foo"');
 * 
 * * [shortcut] - the table shortcut to prefix the field (column name).
 * If specified, the result will be `T."field1",T."field2"` if [shortcut] is `T`.
 * Note: [shortcut] is case insensitive.
 */
String sqlColumns(Iterable<String> fields, [String shortcut]) {
  if (fields == null)
    return "*";
  if (fields.isEmpty)
    return '1';

  final StringBuffer sql = new StringBuffer();
  bool first = true;
  for (final String field in fields) {
    if (first) first = false;
    else sql.write(',');

    if (StringUtil.isChar(field[0], digit: true, match: "(")) {
      sql.write(field);
    } else {
      if (shortcut != null)
        sql..write(shortcut)..write('.');
      sql..write('"')..write(field)..write('"');
    }
  }
  return sql.toString();
}

/** Returns the where criteria (without where) by anding [whereValues].
 * 
   * * [option] - whether to use [FOR_SHARE], [FOR_UPDATE]
   * or null (default; no lock).
 */
String sqlWhereBy(Map<String, dynamic> whereValues, [int option, String append]) {
  final StringBuffer where = new StringBuffer();
  bool first = true;
  for (final String name in whereValues.keys) {
    if (first) first = false;
    else where.write(' and ');

    where..write('"')..write(name);

    if (whereValues[name] != null)
      where..write('"=@')..write(name);
    else
      where.write('" is null');
  }

  if (append != null)
    where..write(' ')..write(append);
  if (option == FOR_UPDATE)
    where.write(' for update');
  else if (option == FOR_SHARE)
    where.write(' for share');
  return where.toString();
}

/** Configures the access library.
 * 
 * Note: it must be called with a non-null pool before calling [access]
 * to start a transaction.
 * 
 * * [pool] - the pool used to establish a connection
 * * [slowSql] - how long to consider a query or an execution is slow.
 * It is used to detect if any slow SQL statement. Default: null (no detect).
 * * [onSlowSql] - if specified, it is called when a slow query is detected.
 * If not, the SQL statement will be logged.
 * * [getErrorMessage] - if specified, it is called to retrieve
 * a human readable message of the given [sql] and [values] when an error occurs.
 * Default: it returns a string concatenating [sql] and [values].
 * * [onTag] - once [tag] is called (with a non-null value), [onTag]
 * will be called in the next invocation of [execute] or [query]
 * (i.e., the next SQL statement). If not specified, the SQL statement
 * is simply logged.
 * * [shallLogError] - test if the given exception shall be logged.
 * Default: always true. You can turn the log off by returning false.
 * 
 * * It returns the previous pool, if any.
 */
Pool configure(Pool pool, {Duration slowSql,
    void onSlowSql(DBAccess access, Duration timeSpent,
          String sql, Map<String, dynamic> values),
    String getErrorMessage(String sql, values),
    void onTag(DBAccess access, cause, String sql, Map<String, dynamic> values),
    bool shallLogError(DBAccess access, ex)}) {
  final p = _pool;
  _pool = pool;
  _slowSql = slowSql;
  _onSlowSql = onSlowSql;
  _onTag = onTag;
  _getErrorMessage = getErrorMessage ?? _defaultErrorMessage;
  _shallLogError = shallLogError ?? _defaultShallLog;
  return p;
}
Pool _pool;
Duration _slowSql;

typedef void _OnSlowSql(DBAccess access, Duration timeSpent, String sql, Map<String, dynamic> values);
_OnSlowSql _onSlowSql;

typedef void _OnTag(DBAccess access, cause, String sql, Map<String, dynamic> values);
_OnTag _onTag;

typedef String _GetErrorMessage(String sql, values);
_GetErrorMessage _getErrorMessage;
String _defaultErrorMessage(String sql, values) => "$sql, $values";

typedef bool _ShallLog(DBAccess access, ex);
_ShallLog _shallLogError;
bool _defaultShallLog(DBAccess access, ex) => true;
