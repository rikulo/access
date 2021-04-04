//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Thu, Jul 03, 2014  4:43:13 PM
// Author: tomyeh
library access;

import "dart:async";
import "dart:collection" show LinkedHashSet, HashMap;

import "package:logging/logging.dart" show Logger;
import "package:postgresql2/postgresql.dart";
import "package:postgresql2/pool.dart";
import "package:entity/postgresql2.dart";
import "package:entity/entity.dart";
import "package:rikulo_commons/util.dart";

export "package:postgresql2/postgresql.dart"
  show Connection, PostgresqlException, Row;

final _logger = Logger("access");

const String
  pgSuccessfulCompletion = "00000",
  pgWarning = "010000",
  pgNoData = "020000",
  pgDuplicateTable = "42P07",
  pgFailedInTransaction = "25P02",
  pgUndefinedObject = "42704",
  pgUndefinedTable = "42P01",
  pgIntegrityConstraintViolation = "23000",
  pgNotNullViolation = "23502",
  pgForeignKeyViolation = "23503",
  pgUniqueViolation = "23505",
  pgCheckViolation = "23514";

/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate a field shall not be the given value,
/// so-called negative condition.
/// 
/// Example,
/// ```
///    await access.queryBy(..., {fdRemovedAt: not(null), fdType: not(1)});
/// ```
Not<T> not<T>(T value) => Not<T>(value);

/// Used in the `whereValues` to represent a not-null condition.
const notNull = const Not(null);

/// Used in the `whereValues` to indicate a negative condition.
/// 
/// In most cases, you shall use [not] instead for its simplicity.
/// 
/// Use [Not] only for constructing a constant conditions:
/// ```
/// const {
///   "foo": const Not(null),
///   "key": const Not("abc"),
/// }
/// ```
class Not<T> {
  final T value;
  const Not(T this.value);
}

///Whether it is [PostgresqlException] about the violation of the given [code].
bool isViolation(ex, String code)
=> ex is PostgresqlException && ex.serverMessage != null
    && ex.serverMessage.code == code;

///Whether it is [PostgresqlException] about the violation of uniqueness.
///It is useful with select-for-update
bool isUniqueViolation(ex) => isViolation(ex, pgUniqueViolation);
///Whether it is [PostgresqlException] about the violation of foreign keys.
bool isForeignKeyViolation(ex) => isViolation(ex, pgForeignKeyViolation);
///Whether it is [PostgresqlException] about the violation of foreign keys.
bool isNotNullViolation(ex) => isViolation(ex, pgNotNullViolation);

/// Returns the number of accesses being executing.
///
/// Note: it can be larger than the real number of DB connections
/// (i.e., `busyConnectionCount`), since the number is increased when
/// [access] is called, i.e., before a DB connection is established.
/// This number indicates it more accurate that the system is busy.
int get accessCount => _nAccess;
int _nAccess = 0;

/** Executes a command within a transaction.
 * 
 *    access((DBAccess access) async {
 *      await for (final Row row in await access.query('select ...')) {
 *        ...
 *      }
 *      ...
 *      await access.execute('update...');
 *    });
 *    //The transaction ends here. It commits if success,
 *    //Or, rolls back if an exception is thrown or [DBAccess.rollingback]
 *    //is set to a value other than false and null.
 *
 * It returns what was returned by [command].
 */
Future<T> access<T>(FutureOr<T> command(DBAccess access)) async {
  var error;
  bool closing = false;
  DBAccess access;

  ++_nAccess;
  try {
    access = DBAccess._(await _pool.connect());
    await access._begin();

    final result = await command(access);

    closing = true;
    if (!access._closed)
      if (access.rollingback == false) {
        await access._commit();
      } else {
        error = access.rollingback; //yes, use it as an error
        await access._rollback();
      }

    return result;

  } catch (ex) {
    error = ex;
    if (access != null && !access._closed && !closing)
      await access._rollback()
      .catchError(_rollbackError);
    rethrow;

  } finally {
    if (access != null && !access._closed)
      access._close(error); //never throws an exception
    --_nAccess;
  }
}

void _rollbackError(ex, StackTrace st)
=> _logger.warning("Failed to rollback", ex, st);

_asNull(_) => null;

bool _isStateError(ex) => ex is StateError;

typedef _ErrorTask(error);
typedef _Task();

/// The database access transaction.
/// It is designed to used with [access].
class DBAccess extends PostgresqlAccess {
  Map<String, dynamic> _dataset;
  List<_Task> _afterCommits;
  List<_ErrorTask> _afterRollbacks;
  bool _closed = false, //whether it is closed
    _beginCounted = false; //whether [begin] is called; used to maintain [_nAccess]
  var _error; //available only if [closed]

  /// Whether this transaction is closed.
  bool get closed => _closed;

  /// Starts an transactions.
  /// You don't need to call this method if you're using [access].
  /// 
  /// If you prefer to handle the transaction explicitly, you *MUST* do
  /// as follows.
  /// 
  ///     final access = await DBAccess.begin();
  ///     try {
  ///        ...
  ///     } catch (ex) {
  ///       access.rollingback = true;
  ///       //rethrow or handle it
  ///     } finally {
  ///       await access.close();
  ///     }
  static Future<DBAccess> begin() async {
    DBAccess access;
    ++_nAccess; //increase first, so [currentAccessCount] more accurate
    try {
      access = DBAccess._(await _pool.connect());
      await access._begin();
      access._beginCounted = true;
      return access;
    } catch (ex) {
      access?._close(ex); //never throws an exception
      --_nAccess;
      rethrow;
    }
  }

  /// Forces the transaction to close immediately.
  /// You rarely need to call this method, since the transaction
  /// will be closed automatically by [access].
  /// 
  /// After calling this method, you cannot access this transaction any more.
  /// 
  /// This method will check if [rollingback] is specified with a non-false
  /// value. If so, it will roll back.
  Future close() async {
    if (_closed) throw StateError('closed');

    var error;
    try { 
      if (rollingback == false) {
        await _commit();
      } else {
        error = rollingback; //yes, use it as an error
        await _rollback();
      }
    } catch (ex) {
      error = ex;
      await _rollback()
      .catchError(_rollbackError);

      rethrow;
    } finally {
      _close(error); //never throws an exception
      if (_beginCounted) --_nAccess;
    }
  }

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
  dynamic _rollingback = false;

  /// Whether this transaction was marked as rollback.
  /// It actually returns `rollingback != false`.
  bool get isRollingback => _rollingback != false;

  DBAccess._(Connection conn): super(conn, cache: true);

  /// How long to consider the query or execution of a SQL statement is slow.
  /// If not specified (i.e., null), the value specified in [configure] is used.
  Duration slowSqlThreshold;

  /// A map of application-specific data.
  Map<String, dynamic> get dataset
  => _dataset != null ? _dataset: MapUtil.auto<String, dynamic>(
          () => _dataset = HashMap<String, dynamic>());

  /** Adds a task that will be executed after the transaction is committed
   * successfully.
   * Note: [task] will be executed directly if the transaction was committed.
   */
  void afterCommit(FutureOr task()) {
    assert(task != null);
    if (_closed) {
      if (_error == null)
        Timer.run(() => _invokeSafely(task));
      return;
    }

    if (_afterCommits == null)
      _afterCommits = <_Task>[];
    _afterCommits.add(task);
  }

  /** Adds a task that will be executed after the transaction is rolled back.
   * Note: [task] will be executed directly if the transaction was rolled back.
   */
  void afterRollback(FutureOr task(error)) {
    assert(task != null);
    if (_closed) {
      if (_error != null)
        Timer.run(() => _invokeSafelyWith(task, _error));
      return;
    }

    if (_afterRollbacks == null)
      _afterRollbacks = <_ErrorTask>[];
    _afterRollbacks.add(task);
  }

  void _close(error) {
    assert(!_closed);
    _closed = true;
    _error = error;
    try {
      conn.close();
    } catch (ex, st) {
      _logger.warning("Failed to close", ex, st);
    }

    if (error != null) {
      if (_afterRollbacks != null)
        Timer.run(() async {
          for (final task in _afterRollbacks)
            await _invokeSafelyWith(task, error);
        });
    } else {
      if (_afterCommits != null)
        Timer.run(() async {
          for (final task in _afterCommits)
            await _invokeSafely(task);
        });
    }
  }

  static Future _invokeSafely(FutureOr task()) async {
    try {
      await task();
    } catch (ex, st) {
      _logger.warning("Failed to invoke $task", ex, st);
    }
  }
  static Future _invokeSafelyWith(FutureOr task(error), error) async {
    try {
      await task(error);
    } catch (ex, st) {
      _logger.warning("Failed to invoke $task with $error", ex, st);
    }
  }

  /// Queues a command for execution, and when done, returns the number of rows
  /// affected by the SQL command.
  @override
  Future<int> execute(String sql, [values]) async {
    if (_closed)
      throw StateError("Closed: ${_getErrorMessage(sql, values)}");

    final tmPreSlow = _startSql();
    try {
      final result = await conn.execute(sql, values);
      _checkSlowSql(sql, values);
      return result;

    } catch (ex, st) {
      if (_shallLogError(this, ex))
        _logger.severe("Failed to execute: ${_getErrorMessage(sql, values)}", ex, st);
      rethrow;

    } finally {
      tmPreSlow?.cancel();
    }
  }

  /// Queue a SQL query to be run, returning a [Stream] of rows.
  @override
  Stream<Row> query(String sql, [values]) {
    if (_closed)
      throw StateError("Closed: ${_getErrorMessage(sql, values)}");

    final controller = StreamController<Row>(),
      tmPreSlow = _startSql();
    conn.query(sql, values)
      .listen((data) => controller.add(data),
        onError: (ex, StackTrace st) {
          controller.addError(ex, st);
          tmPreSlow?.cancel();

          if (_shallLogError(this, ex))
            _logger.severe("Failed to query: ${_getErrorMessage(sql, values)}", ex, st);
        },
        onDone: () {
          controller.close();

          _checkSlowSql(sql, values);
          tmPreSlow?.cancel();
        },
        cancelOnError: true);
    return controller.stream;
  }

  /// Called before executing a SQL statement.
  Timer _startSql() {
    if (_defaultSlowSqlThreshold != null || slowSqlThreshold != null) {
      _sqlStartAt = DateTime.now();

      if (_onPreSlowSql != null)
        return Timer(_calcPreSlowSql(slowSqlThreshold) ??
            _defaultPreSlowSqlThreshold, _onPreSlowSqlTimeout);
            //Don't use execute().timeout() to avoid any error zone issue
    }
    return null;
  }

  void _onPreSlowSqlTimeout() async {
    Connection conn;
    try {
      conn = await _pool.connect(); //use a separated transaction

      final rows = await conn.query("""
  SELECT BdLk.pid, age(now(), BdAc.query_start), BdAc.query,
  BiLk.pid, age(now(), BiAct.query_start), BiAct.query
  FROM pg_catalog.pg_locks BdLk
  JOIN pg_catalog.pg_stat_activity BdAc ON BdAc.pid = BdLk.pid
  JOIN pg_catalog.pg_locks BiLk 
  ON BiLk.locktype = BdLk.locktype
  AND BiLk.DATABASE IS NOT DISTINCT FROM BdLk.DATABASE
  AND BiLk.relation IS NOT DISTINCT FROM BdLk.relation
  AND BiLk.page IS NOT DISTINCT FROM BdLk.page
  AND BiLk.tuple IS NOT DISTINCT FROM BdLk.tuple
  AND BiLk.virtualxid IS NOT DISTINCT FROM BdLk.virtualxid
  AND BiLk.transactionid IS NOT DISTINCT FROM BdLk.transactionid
  AND BiLk.classid IS NOT DISTINCT FROM BdLk.classid
  AND BiLk.objid IS NOT DISTINCT FROM BdLk.objid
  AND BiLk.objsubid IS NOT DISTINCT FROM BdLk.objsubid
  AND BiLk.pid != BdLk.pid
  JOIN pg_catalog.pg_stat_activity BiAct ON BiAct.pid = BiLk.pid
  WHERE NOT BdLk.GRANTED""").toList();

      if (_onPreSlowSql == null) return; //just in case

      String msg;
      if (rows.isEmpty) msg = "None";
      else {
        final buf = StringBuffer();
        int i = 0;
        for (final r in rows) {
          if (i++ > 0) buf.write('\n');

          buf..write("Blocked ")
            ..write(r[0])..write(": ")..write(r[1])..write(' ')..writeln(r[2])
            ..write("Blocking ")
            ..write(r[3])..write(": ")..write(r[4])..write(' ')..write(r[5]);
        }
        msg = buf.toString();
      }

      await _onPreSlowSql(conn, dataset, msg);

    } catch (ex, st) {
      _logger.warning("Unable to onPreSlowSql", ex, st);
    } finally {
      conn?.close();
    }
  }

  /// Checks if the execution is taking too long. If so, logs it.
  void _checkSlowSql(String sql, dynamic values) {
    if (_sqlStartAt != null) {
      final spent = DateTime.now().difference(_sqlStartAt),
        threshold = slowSqlThreshold ?? _defaultSlowSqlThreshold;
      if (threshold != null && spent > threshold)
        _onSlowSql(dataset, spent,
            sql == 'commit' && _lastSql != null ? "commit: $_lastSql": sql,
            values);
        //unlike _onPreSlowSql, _onSlowSql never null
    }
    _lastSql = sql;
  }
  ///The last executed SQL. Used for logging slow SQL.
  String _lastSql;
  ///When the last SQL was executed. Used for logging slow SQL.
  DateTime _sqlStartAt;

  ///Returns the first result, or null if not found.
  Future<Row> queryAny(String sql, [values])
  => query(sql, values).first
    .catchError(_asNull, test: _isStateError);

  /** Queries [fields] of [otype] for the criteria specified in
   * [whereValues] (AND-ed together).
   * 
   * * [option] - whether to use [forUpdate], [forShare] or null.
   */
  Stream<Row> queryBy(Iterable<String> fields, String otype,
    Map<String, dynamic> whereValues, [int option])
  => _queryBy(fields, otype, whereValues, option, null);

  Stream<Row> _queryBy(Iterable<String> fields, String otype,
    Map<String, dynamic> whereValues, int option, String append)
  => queryWith(fields, otype,
      sqlWhereBy(whereValues, append), whereValues, null, null, option);

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
   * > If you'd like *select-for-update*, you can specify [forUpdate]
   * or [forShare] to [option].
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$fdType"=23`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$otTask" inner join "$otGrant"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   * 
   * Note: if [fromClause] is specified, [otype] is ignored.
   */
  Stream<Row> queryWith(Iterable<String> fields, String otype,
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut, int option]) {
    String sql = 'select ${sqlColumns(fields, shortcut)} from ';
    sql += fromClause != null ? fromClause:
        shortcut != null ? '"$otype" $shortcut': '"$otype"';
    if (whereClause != null)
      sql += ' where $whereClause';
    if (option == forUpdate)
      sql += ' for update';
    else if (option == forShare)
      sql += ' for share';
    return query(sql, whereValues);
  }

  /** Returns the first result, or null if not found.
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$fdType" = 23`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$otTask" inner join "$otGrant"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<Row> queryAnyWith(Iterable<String> fields, String otype,
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut, int option])
  => queryWith(fields, otype, whereClause, whereValues, fromClause, shortcut, option)
    .first
    .catchError(_asNull, test: _isStateError);

  ///Loads the entity by the given [oid], or null if not found.
  Future<T> load<T extends Entity>(
      Iterable<String> fields, T newInstance(String oid), String oid,
      [int option])
  => loadIfAny(this, oid, newInstance, fields, option);

  /** Loads all entities of the given criteria (never null).
   * 
   * * [whereClause] - if null, no where clause is generated.
   * That is, the whole table will be loaded.
   * Note: it shall not include `where`.
   * Example: `"$fdType" = 23`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$otTask" inner join "$otGrant"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<List<T>> loadAllWith<T extends Entity>(
      Iterable<String> fields, T newInstance(String oid),
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut, int option]) async {
    Set<String> fds;
    if (fields != null) {
      fds = LinkedHashSet<String>();
      fds..add(fdOid)..addAll(fields);
    }

    final entities = <T>[];
    await for (final row in
        queryWith(fds, fromClause != null ? fromClause: newInstance('*').otype,
        whereClause, whereValues, fromClause, shortcut, option)) {
      entities.add(await toEntity(row, fields, newInstance));
    }
    return entities;
  }

  /** Instantiates an Entity instance to represent the data in [row].
   * If [row] is, this method will return `Future.value(null)`.
   */
  Future<T> toEntity<T extends Entity>(Row row, Iterable<String> fields,
      T newInstance(String oid)) {
    if (row == null)
      return Future.value();

    final data = HashMap<String, dynamic>();
    row.forEach((String name, value) => data[name] = value);
    assert(data.containsKey(fdOid)); //fdOid is required.
    return loadIfAny_(this, data.remove(fdOid) as String, newInstance,
        (e, fds, option) => data, fields);
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
   * Example: `"$fdType" = 23`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$otTask" inner join "$otGrant"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<List<T>> loadWhile<T extends Entity>(
      Iterable<String> fields, T newInstance(String oid),
      bool test(T lastLoaded, List<T> loaded),
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut, int option]) async {

    final loaded = <T>[];

    await for (final Row row in queryWith(
        fields != null ? (LinkedHashSet.from(fields)..add(fdOid)): null,
        fromClause != null ? fromClause: newInstance('*').otype,
        whereClause, whereValues, fromClause, shortcut, option)) {

      final T e = await toEntity(row, fields, newInstance);
      loaded.add(e); //always add (i.e., add before test)
      if (!test(e, loaded))
        break;
    }

    return loaded;
  }

  /** Loads the first entity of the given criteria, or returns null if none.
   * 
   * * [whereClause] - the where clause.
   * Note: it shall not include `where`.
   * Example: `"$fdType" = 23`
   * * [fromClause] - if null, the entity's table is assumed.
   * Note: it shall not include `from`.
   * Example: `"$otTask" inner join "$otGrant"`
   * * [shortcut] - the table shortcut to prefix the column names.
   * Default: none. Useful if you joined other tables in [fromClause].
   * Note: [shortcut] is case insensitive.
   */
  Future<T> loadWith<T extends Entity>(
      Iterable<String> fields, T newInstance(String oid),
      String whereClause, [Map<String, dynamic> whereValues,
      String fromClause, String shortcut, int option]) async {
    Set<String> fds;
    if (fields != null) {
      fds = LinkedHashSet<String>();
      fds..add(fdOid)..addAll(fields);
    }

    Row row;
    try {
      row = await queryWith(fds,
        fromClause != null ? fromClause: newInstance('*').otype,
        whereClause, whereValues, fromClause, shortcut, option).first;
    } on StateError catch (_) {
      //ignore
    }

    return toEntity(row, fields, newInstance);
  }

  /** Loads all entities of the given AND criteria.
   * By AND, we mean it satisfies all values in [whereValues].
   * 
   * * [option] - whether to use [forShare], [forUpdate]
   * or null (default; no lock).
   */
  Future<List<T>> loadAllBy<T extends Entity>(
      Iterable<String> fields, T newInstance(String oid),
      Map<String, dynamic> whereValues, [int option])
  => loadAllWith(fields, newInstance,
      sqlWhereBy(whereValues), whereValues, null, null, option);

  /** Loads the first entity of the given AND criteria.
   * By AND, we mean it satisfies all values in [whereValues].
   * 
   * * [option] - whether to use [forShare], [forUpdate]
   * or null (default; no lock).
   */
  Future<T> loadBy<T extends Entity>(
      Iterable<String> fields, T newInstance(String oid),
      Map<String, dynamic> whereValues, [int option])
  => loadWith(fields, newInstance,
      sqlWhereBy(whereValues, "limit 1"), whereValues, null, null, option);

  ///Deletes the entity of the given [oid].
  Future<int> delete(String otype, String oid) {
    uncache(otype, oid);
    return execute('delete from "$otype" where "$fdOid"=@$fdOid',
      {fdOid: oid});
  }
  /** Inserts the entity specified in data.
   * Note: all fields found in [data] are written. You have to
   * remove unnecessary files by yourself, such as [fdOtype].
   * 
   * * [types] - a map of (field-name, field-type). If specified,
   * the type of the field will be retrieved from [types], if any.
   * * [append] - the extra clause to append to the insert statement.
   * Example, `final oid = await insert(..., append: returning "$fdOid");`
   */
  Future<dynamic> insert(String otype, Map<String, dynamic> data,
      {Map<String, String> types, String append}) {
    final sql = StringBuffer('insert into "')..write(otype)..write('"('),
      param = StringBuffer(" values(");

    bool first = true;
    for (final fd in data.keys) {
      if (first) first = false;
      else {
        sql.write(',');
        param.write(',');
      }
      sql..write('"')..write(fd)..write('"');

      param..write('@')..write(fd);
      if (types != null) {
        final type = types[fd];
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
    final stmt = sql.toString();
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
  final result = [];
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

  final sql = StringBuffer();
  bool first = true;
  for (final field in fields) {
    if (first) first = false;
    else sql.write(',');

    if (_reExpr.hasMatch(field)) {
      sql.write(field);
    } else {
      if (shortcut != null)
        sql..write(shortcut)..write('.');
      sql..write('"')..write(field)..write('"');
    }
  }
  return sql.toString();
}
final _reExpr = RegExp(r'(^[0-9]|[("+])');

/** Returns the where criteria (without where) by anding [whereValues].
 * 
 * Note: if a value in [whereValues] is null, it will generate
 * `foo is null`. If a value is [not], it will generate `!=`.
 * Example, `"foo": not(null)` => `foo is not null`.
 * `"foo": not(123)` => `foo != 123`.
 */
String sqlWhereBy(Map<String, dynamic> whereValues, [String append]) {
  final where = StringBuffer();
  bool first = true;
  for (final name in whereValues.keys) {
    if (first) first = false;
    else where.write(' and ');

    where..write('"')..write(name);

    var value = whereValues[name];
    bool negate;
    if (negate = value is Not) value = value.value;

    if (value != null) {
      where.write('"');
      if (negate) where.write('!');
      where..write('=@')..write(name);
    } else {
      where.write('" is ');
      if (negate) where.write("not ");
      where.write('null');
    }
  }

  if (append != null)
    where..write(' ')..write(append);
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
 * * [getErrorMessage] - if specified, it is called to retrieve
 * a human readable message of the given [sql] and [values] when an error occurs.
 * Default: it returns a string concatenating [sql] and [values].
 * * [shallLogError] - test if the given exception shall be logged.
 * Default: always true. You can turn the log off by returning false.
 * 
 * * It returns the previous pool, if any.
 */
Pool configure(Pool pool, {Duration slowSqlThreshold,
    void onSlowSql(Map<String, dynamic> dataset, Duration timeSpent, String sql, dynamic values),
    FutureOr onPreSlowSql(Connection conn, Map<String, dynamic> dataset, String message),
    String getErrorMessage(String sql, dynamic values),
    bool shallLogError(DBAccess access, ex)}) {
  final p = _pool;
  _pool = pool;
  _defaultPreSlowSqlThreshold = _calcPreSlowSql(
      _defaultSlowSqlThreshold = slowSqlThreshold);
  _onSlowSql = onSlowSql ?? _defaultOnSlowSql;
  _onPreSlowSql = onPreSlowSql;
  _getErrorMessage = getErrorMessage ?? _defaultErrorMessage;
  _shallLogError = shallLogError ?? _defaultShallLog;
  return p;
}
Pool _pool;
///How long to consider an execution slow
Duration _defaultSlowSqlThreshold,
///How long to log locking and other info (95% of [_defaultSlowSqlThreshold])
  _defaultPreSlowSqlThreshold;

Duration _calcPreSlowSql(Duration dur)
=> dur == null ? null: Duration(microseconds: (dur.inMicroseconds * 95) ~/ 100);

void Function(Map<String, dynamic> dataset, Duration timeSpent, String sql, dynamic values)
  _onSlowSql;
FutureOr Function(Connection conn, Map<String, dynamic> dataset, String message)
  _onPreSlowSql;

String Function(String sql, dynamic values) _getErrorMessage;
String _defaultErrorMessage(String sql, dynamic values) => sql;

void _defaultOnSlowSql(Map<String, dynamic> dataset, Duration timeSpent,
    String sql, var values) {
  _logger.warning("Slow SQL ($timeSpent): $sql");
}
typedef bool _ShallLog(DBAccess access, ex);
_ShallLog _shallLogError;
bool _defaultShallLog(DBAccess access, ex) => true;
