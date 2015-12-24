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
 * It returns what was returned by [command].
 * 
 * Unlike [DBAccess.begin], this method will commit and rollback automatically.
 */
Future access(command(DBAccess access)) {
  var result;
  DBAccess access;
  return _pool.connect()
  .then((Connection conn) {
    access = new DBAccess(conn);
    return access._begin();
  })
  .then((_) => result = command(access))
  .then((_) => access._commit())
  .then((_) => result)
  .catchError(
    (ex, st) => access._rollback()
    .catchError((ex2, st2) => _logger.warning("Failed to rollback", ex2, st2))
    .then((_) => new Future.error(ex, st)),
    test: (ex) => access != null
  )
  .whenComplete(() {
    if (access != null) {
      access._closed = true;
      access.conn.close();
    }
  });
}

/** The database access.
 */
class DBAccess extends PostgresqlAccess {
  Map<String, dynamic> _dataset;
  var _tag;
  bool _closed = false;

  DBAccess(Connection conn): super(conn, cache: true);

  /** A map of application-specific data.
   */
  Map<String, dynamic> get dataset
  => _dataset != null ? _dataset: MapUtil.auto(() => _dataset = new HashMap());

  ///Tags the next SQL statement in this access (aka., transaction).
  ///Once tagged, [onTag] will be called in the next invocation
  ///of [query] or [execute]. If [onTag] is not specified, the SQL statement
  ///is simply logged.
  void tag(cause) {
    _tag = cause;
  }

  /// Queues a command for execution, and when done, returns the number of rows
  /// affected by the SQL command.
  @override
  Future<int> execute(String sql, [values]) {
    if (_closed)
      throw new StateError(sql);

    _checkTag(sql, values);

    Future op;
    if (_slowSql != null) {
      final DateTime started = new DateTime.now();
      op = conn.execute(sql, values)
        .then((result) {
          _checkSlowSql(started, sql, values);
          return result;
        });
    } else {
      op = conn.execute(sql, values);
    }

    return op.catchError((ex, st) {
      _logger.severe("Failed execute: ${_getErrorMessage(sql, values)}", ex, st);
      return new Future.error(ex, st);
    }, test: _shallLogError);
  }

  /// Queue a SQL query to be run, returning a [Stream] of rows.
  @override
  Stream<Row> query(String sql, [values]) {
    if (_closed)
      throw new StateError("Closed: ${_getErrorMessage(sql, values)}");

    _checkTag(sql, values);

    final StreamController controller = new StreamController();
    final DateTime started = _slowSql != null ? new DateTime.now(): null;
    conn.query(sql, values)
      .listen((Row data) => controller.add(data),
        onError: (ex, st) {
          if (_shallLogError(ex))
            _logger.severe("Failed query: ${_getErrorMessage(sql, values)}", ex, st);
          controller.addError(ex, st);
        },
        onDone: () {
          controller.close();

          if (_slowSql != null)
            _checkSlowSql(started, sql, values);
        },
        cancelOnError: true);
    return controller.stream;
  }

  ///Check if it is tagged.
  void _checkTag(String sql, [values]) {
    if (_tag != null) {
      final tag = _tag;
      _tag = null; //do it only once
      if (_onTag != null)
        _onTag(tag, sql, values);
      else
        _logger.warning('[$tag] SQL: ${_defaultErrorMessage(sql, values)}');
    }
  }
  ///Checks if it is slow. If so, logs it.
  void _checkSlowSql(DateTime started, String sql, [values]) {
    final Duration spent = new DateTime.now().difference(started);
    if (spent > _slowSql) {
      tag("after slow");
      if (_onSlowSql != null)
        _onSlowSql(spent, sql, values);
      else
        _logger.warning('Slow SQL ($spent): ${_getErrorMessage(sql, values)}');
    }
  }

  ///Returns the first result, or null if not found.
  Future<Row> queryAny(String sql, [values])
  => query(sql, values).first
    .catchError((ex) => null, test: (ex) => ex is StateError);

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
    .catchError((ex) => null, test: (ex) => ex is StateError);

  /** Queries [fields] of [otype] for the criteria specified in
   * [whereClause] and [whereValues].
   * 
   * > If you'd like *select-for-update*, you can put `for update`
   * or `for share` to [whereClause].
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   */
  Stream<Row> queryWith(Iterable<String> fields, String otype,
      String whereClause, [Map<String, dynamic> whereValues]) {
    String sql = 'select ${sqlColumns(fields)} from "$otype"';
    if (whereClause != null)
      sql += ' where $whereClause';
    return query(sql, whereValues);
  }

  /** Returns the first result, or null if not found.
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   */
  Future<Row> queryAnyWith(Iterable<String> fields, String otype,
      String whereClause, [Map<String, dynamic> whereValues])
  => queryWith(fields, otype, whereClause, whereValues).first
    .catchError((ex) => null, test: (ex) => ex is StateError);

  ///Loads the entity by the given [oid], or null if not found.
  Future<Entity> load(
      Iterable<String> fields, Entity newInstance(String oid), String oid,
      [int option])
  => loadIfAny(this, oid, newInstance, fields, option);

  /** Loads all entities of the given criteria (never null).
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   */
  Future<List<Entity>> loadAllWith(
      Iterable<String> fields, Entity newInstance(String oid),
      String whereClause, [Map<String, dynamic> whereValues]) {
    Set<String> fds;
    if (fields != null) {
      fds = new HashSet();
      fds..add(F_OID)..addAll(fields);
    }

    final String otype = newInstance('*').otype;
    return queryWith(fds, otype, whereClause, whereValues).toList()
    .then((List<Row> rows) {
      final List<Entity> entities = [];
      return Future.forEach(rows,
        (Row row) {
          return toEntity(row, fields, newInstance)
          .then((Entity entity) {
            entities.add(entity);
          });
        })
      .then((_) => entities);
    });
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
   */
  Future<List<Entity>> loadWhile(
      Iterable<String> fields, Entity newInstance(String oid),
      bool test(Entity lastLoaded, List<Entity> loaded),
      String whereClause, [Map<String, dynamic> whereValues]) {

    final Completer<List<Entity>> completer = new Completer();
    final List<Entity> loaded = [];
    final Stream<Row> stream = queryWith(
        fields != null ? (new HashSet.from(fields)..add(F_OID)): null,
        newInstance('*').otype, whereClause, whereValues);

    StreamSubscription subscr;
    subscr = stream.listen(
      (Row row) {
        return toEntity(row, fields, newInstance)
        .then((Entity e) {
          loaded.add(e); //always add and add first

          if (!test(e, loaded)) {
            final result = subscr.cancel();
            if (result is Future)
              result.whenComplete(() => completer.complete(loaded));
            else
              completer.complete(loaded);
          }
        });
      },
      onError: (ex, st) => completer.completeError(ex, st),
      onDone: () => completer.complete(loaded),
      cancelOnError: true);
    return completer.future;
  }

  /** Loads the first entity of the given criteria, or returns null if none.
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   */
  Future<Entity> loadWith(
      Iterable<String> fields, Entity newInstance(String oid),
      String whereClause, [Map<String, dynamic> whereValues]) {
    Set<String> fds;
    if (fields != null) {
      fds = new HashSet();
      fds..add(F_OID)..addAll(fields);
    }

    final String otype = newInstance('*').otype;
    return queryWith(fds, otype, whereClause, whereValues).first
    .catchError((ex) => null, test: (ex) => ex is StateError)
    .then((Row row) => toEntity(row, fields, newInstance));
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
   * Example, `insert(..., append: returning "$F_OID").then((oid) => ...)`
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
      return query(stmt, data).first.then((Row r) => r[0]);

    return execute(stmt, data);
  }

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
 * if [fields] is empty, `1` is returned (so it is easier to construct a SQL statement).
 * 
 * Example,
 * 
 *     access.query('select ${sqlColumns(fields)} from "Foo"');
 */
String sqlColumns(Iterable<String> fields) {
  if (fields == null)
    return "*";
  if (fields.isEmpty)
    return '1';

  final StringBuffer sql = new StringBuffer();
  bool first = true;
  for (final String field in fields) {
    if (first) first = false;
    else sql.write(',');
    sql..write('"')..write(field)..write('"');
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
    void onSlowSql(Duration timeSpent, String sql, Map<String, dynamic> values),
    String getErrorMessage(String sql, values),
    void onTag(cause, String sql, Map<String, dynamic> values),
    bool shallLogError(ex)}) {
  final p = _pool;
  _pool = pool;
  _slowSql = slowSql;
  _onSlowSql = onSlowSql;
  _getErrorMessage = getErrorMessage ?? _defaultErrorMessage;
  _shallLogError = shallLogError ?? _defaultShallLog;
  return p;
}
Pool _pool;
Duration _slowSql;

typedef void _OnSlowSql(Duration timeSpent, String sql, Map<String, dynamic> values);
_OnSlowSql _onSlowSql;

typedef void _OnTag(cause, String sql, Map<String, dynamic> values);
_OnTag _onTag;

typedef String _GetErrorMessage(String sql, values);
_GetErrorMessage _getErrorMessage;
String _defaultErrorMessage(String sql, values) => "$sql, $values";

typedef bool _ShallLog(ex);
_ShallLog _shallLogError;
bool _defaultShallLog(ex) => true;
