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
import "package:rikulo_commons/async.dart";

export "package:postgresql2/postgresql.dart"
  show Connection, PostgresqlException, Row;

part "src/access/util.dart";
part "src/access/configure.dart";

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
 *      await for (final row in await access.query('select ...')) {
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
  Object? error;
  bool closing = false;
  DBAccess? access;

  ++_nAccess;
  try {
    onAccess?.call(command, _nAccess);

    access = DBAccess._(await _pool!.connect());
    await access._begin();

    final result = await command(access);

    closing = true;
    if (!access._closed) {
      final rollingback = access.rollingback;
      if (rollingback == false) {
        await access._commit();
      } else {
        error = rollingback; //yes, use it as an error
        await access._rollback();
      }
    }

    return result;

  } catch (ex) {
    error = ex;
    if (access != null && !access._closed && !closing)
      await access._rollbackSafely();
    rethrow;

  } finally {
    if (access != null && !access._closed)
      access._close(error); //never throws an exception
    --_nAccess;
  }
}

typedef FutureOr _ErrorTask(error);
typedef FutureOr _Task();

/// The database access transaction.
/// It is designed to used with [access].
class DBAccess extends PostgresqlAccess {
  Map<String, dynamic>? _dataset;
  List<_Task>? _afterCommits;
  List<_ErrorTask>? _afterRollbacks;
  bool _closed = false, //whether it is closed
    _beginCounted = false; //whether [begin] is called; used to maintain [_nAccess]
  Object? _error; //available only if [closed]

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
    DBAccess? access;
    ++_nAccess; //increase first, so [currentAccessCount] more accurate
    try {
      access = DBAccess._(await _pool!.connect());
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
      await _rollbackSafely();
      rethrow;
    } finally {
      _close(error); //never throws an exception
      if (_beginCounted) --_nAccess;
    }
  }

  /// A flag or a cause to indicate the access (aka., the transaction) shall
  /// be rolled back at the end.
  /// 
  /// Default: false.
  /// 
  /// To force [access] rolling back, you can set this flag to any value
  /// other than false (in addition to throwing an exception).
  /// 
  /// If a value other than false is set (such as `true`),
  /// the callback passed to [afterRollback] will be called with this value.
  /// 
  /// Note: [access] will still return the return value of the command function
  /// if a flag is set.
  /// 
  /// You can test if rollingback is set as follows:
  /// 
  ///     if (access.isRollingback) ...
  Object get rollingback => _rollingback;
  /// Sets whether to roll back the access (aka., the transaction).
  void set rollingback(Object rollingback) {
    _rollingback = rollingback;
  }
  Object _rollingback = false;

  /// Whether this transaction was marked as rollback.
  /// It actually returns `rollingback != false`.
  bool get isRollingback => _rollingback != false;

  DBAccess._(Connection conn): super(conn, cache: true);

  /// How long to consider the query or execution of a SQL statement is slow.
  /// If not specified (i.e., null), the value specified in [configure] is used.
  Duration? slowSqlThreshold;

  /// A map of application-specific data.
  Map<String, dynamic> get dataset
  => _dataset ?? (_dataset = MapUtil.auto<String, dynamic>(
          () => _dataset = HashMap<String, dynamic>()));

  /** Adds a task that will be executed after the transaction is committed
   * successfully.
   * Note: [task] will be executed directly if the transaction was committed.
   */
  void afterCommit(FutureOr task()) {
    if (_closed) {
      if (_error == null)
        Timer.run(() => _invokeTask(task));
      return;
    }

    (_afterCommits ??= <_Task>[]).add(task);
  }

  /** Adds a task that will be executed after the transaction is rolled back.
   * Note: [task] will be executed directly if the transaction was rolled back.
   */
  void afterRollback(FutureOr task(error)) {
    if (_closed) {
      if (_error != null)
        Timer.run(() => _invokeTaskWith(task, _error));
      return;
    }

    (_afterRollbacks ??= <_ErrorTask>[]).add(task);
  }

  void _close(error) {
    assert(!_closed);
    _closed = true;
    _error = error;

    try {
      conn.close();
    } catch (ex, st) {
      _logger.severe("Failed to close", ex, st);
    }

    if (error != null) {
      final afterRollbacks = _afterRollbacks;
      if (afterRollbacks != null)
        Timer.run(() async {
          for (final task in afterRollbacks)
            await _invokeTaskWith(task, error);
        });
    } else {
      final afterCommits = _afterCommits;
      if (afterCommits != null)
        Timer.run(() async {
          for (final task in afterCommits)
            await _invokeTask(task);
        });
    }
  }

  /// Queues a command for execution, and when done, returns the number of rows
  /// affected by the SQL command.
  @override
  Future<int> execute(String sql, [Map<String, dynamic>? values]) async {
    if (_closed)
      throw StateError("Closed: ${_getErrorMessage(sql, values)}");

    final tmPreSlow = _startSql();
    try {
      final result = await conn.execute(sql, values);
      _checkSlowSql(sql, values);
      return result;
    } catch (ex, st) {
      if (_shallLogError(this, sql, ex))
        _logger.severe("Failed to execute: ${_getErrorMessage(sql, values)}", ex, st);

      rethrow;
    } finally {
      tmPreSlow?.cancel();
    }
  }

  /// Queue a SQL query to be run, returning a [Stream] of rows.
  @override
  Stream<Row> query(String sql, [Map<String, dynamic>? values]) async* {
    if (_closed) throw StateError("Closed: ${_getErrorMessage(sql, values)}");

    final tmPreSlow = _startSql();
    try {
      await for (final row in conn.query(sql, values)) {
        yield row;
      }
      _checkSlowSql(sql, values);
    } catch (ex, st) {
      if (_shallLogError(this, sql, ex))
        _logger.severe("Failed to query: ${_getErrorMessage(sql, values)}", ex, st);

      rethrow;
    } finally {
      tmPreSlow?.cancel();
    }
  }

  /// Called before executing a SQL statement.
  Timer? _startSql() {
    if (_defaultSlowSqlThreshold != null || slowSqlThreshold != null) {
      _sqlStartAt = DateTime.now();

      if (_onPreSlowSql != null)
        return Timer(_calcPreSlowSql(slowSqlThreshold) ??
            _defaultPreSlowSqlThreshold!, _onPreSlowSqlTimeout);
            //Don't use execute().timeout() to avoid any error zone issue
    }
  }

  void _onPreSlowSqlTimeout() async {
    final onPreSlowSql = _onPreSlowSql;
    if (onPreSlowSql == null) return; //just in case

    Connection? conn;
    try {
      conn = await _pool!.connect(); //use a separated transaction

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

      await onPreSlowSql(conn, dataset, msg);

    } catch (ex, st) {
      _logger.warning("Unable to onPreSlowSql", ex, st);
    } finally {
      conn?.close();
    }
  }

  /// Checks if the execution is taking too long. If so, logs it.
  void _checkSlowSql(String sql, Map<String, dynamic>? values) {
    final sqlStartAt = _sqlStartAt;
    if (sqlStartAt != null) {
      final spent = DateTime.now().difference(sqlStartAt),
        threshold = slowSqlThreshold ?? _defaultSlowSqlThreshold;
      if (threshold != null && spent > threshold)
        _onSlowSql(dataset, spent,
            sql == 'commit' && _lastSql != null ? "$_lastSql > commit": sql,
            values);
        //unlike _onPreSlowSql, _onSlowSql never null
    }
    _lastSql = sql;
  }
  ///The last executed SQL. Used for logging slow SQL.
  String? _lastSql;
  ///When the last SQL was executed. Used for logging slow SQL.
  DateTime? _sqlStartAt;

  ///Returns the first result, or null if not found.
  Future<Row?> queryAny(String sql, [Map<String, dynamic>? values])
  => StreamUtil.first(query(_limit1NS(sql, selectRequired: true), values));

  /// Queries [fields] from [fromClause] for the criteria specified in
  /// [whereValues] (AND-ed together).
  /// 
  /// * [fromClause] - any valid from clause, such as a table name,
  /// an inner join, and so on.
  /// Note: it shall not include `from`.
  /// Example: `Foo`, `"Foo" inner join "Moo" on ref=oid`,
  /// and `"Foo" F`.
  /// * [whereValues] - the values in this map will be encoded
  /// as an SQL condition by [sqlWhereBy].
  /// See [sqlWhereBy] for details.
  /// * [option] - whether to use [forUpdate], [forShare] or null.
  Stream<Row> queryBy(Iterable<String>? fields, String fromClause,
      Map<String, dynamic> whereValues,
      [String? shortcut, AccessOption? option])
  => _queryBy(fields, fromClause, whereValues, shortcut, option, null);

  Stream<Row> _queryBy(Iterable<String>? fields, String fromClause,
      Map<String, dynamic> whereValues, String? shortcut,
      AccessOption? option, String? append)
  => queryFrom(fields, fromClause, sqlWhereBy(whereValues, append),
      whereValues, shortcut, option);

  /// Queries [fields] of [fromClause] for the criteria specified in
  /// [whereValues] (AND-ed together),
  /// and returns only the first row, or null if nothing found.
  ///
  /// > Refer to [queryBy] for details.
  Future<Row?> queryAnyBy(Iterable<String>? fields, String fromClause,
      Map<String, dynamic> whereValues,
      [String? shortcut, AccessOption? option])
  => StreamUtil.first(_queryBy(fields, fromClause, whereValues,
      shortcut, option, "limit 1"));

  /// Queries [fields] from [fromClause] for the criteria specified in
  /// [whereClause] and [whereValues].
  /// 
  /// > If you'd like *select-for-update*, you can specify [forUpdate]
  /// or [forShare] to [option].
  /// 
  /// * [fromClause] - any valid from clause, such as a table name,
  /// an inner join, and so on.
  /// Note: it shall not include `from`.
  /// Example: `Foo`, `"Foo" inner join "Moo" on ref=oid`,
  /// and `"Foo" F`.
  /// * [whereClause] - if null, no where clause is generated.
  /// That is, the whole table will be loaded.
  /// Note: it shall not include `where`.
  /// Example: `"$fdType"=23`
  /// * [shortcut] - the table shortcut to prefix the column names.
  /// Default: none. Useful if you joined other tables in [fromClause].
  /// Note: [shortcut] is case insensitive.
  Stream<Row> queryFrom(Iterable<String>? fields, String fromClause,
      String? whereClause, [Map<String, dynamic>? whereValues,
      String? shortcut, AccessOption? option]) {
    final sql = StringBuffer('select ');
    addSqlColumns(sql, fields, shortcut);
    sql.write(' from ');

    if (_reComplexFrom.hasMatch(fromClause)) sql.write(fromClause);
    else {
      sql..write('"')..write(fromClause)..write('"');
      if (shortcut != null) sql..write(' ')..write(shortcut);
    }

    if (whereClause != null && whereClause.trim().isNotEmpty) {
      if (!_reNoWhere.hasMatch(whereClause))
        sql.write(' where');
      sql..write(' ')..write(whereClause);
    }

    if (option == forUpdate) sql.write(' for update');
    else if (option == forShare) sql.write(' for share');
    return query(sql.toString(), whereValues);
  }
  static final
    _reNoWhere = RegExp(r'^\s*(?:order|group|limit|for)', caseSensitive: false),
    _reComplexFrom = RegExp(r'["\s]');

  /// Returns the first result, or null if not found.
  /// 
  /// * [fromClause] - any valid from clause, such as a table name,
  /// an inner join, and so on.
  /// Note: it shall not include `from`.
  /// Example: `Foo`, `"Foo" inner join "Moo" on ref=oid`,
  /// and `"Foo" F`.
  /// * [whereClause] - if null, no where clause is generated.
  /// That is, the whole table will be loaded.
  /// Note: it shall not include `where`.
  /// Example: `"$fdType" = 23`
  /// * [shortcut] - the table shortcut to prefix the column names.
  /// Default: none. Useful if you joined other tables in [fromClause].
  /// Note: [shortcut] is case insensitive.
  Future<Row?> queryAnyFrom(Iterable<String>? fields, String fromClause,
      String? whereClause, [Map<String, dynamic>? whereValues,
      String? shortcut, AccessOption? option])
  => StreamUtil.first(queryFrom(fields, fromClause,
      _limit1(whereClause, selectRequired: false)
      ?? (_reLimit.hasMatch(fromClause) ? null: 'limit 1'),
      whereValues, shortcut, option));

  ///Loads the entity by the given [oid], or null if not found.
  Future<T?> load<T extends Entity>(
      Iterable<String>? fields, T newInstance(String oid), String? oid,
      [AccessOption? option])
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
      Iterable<String>? fields, T newInstance(String oid),
      String? whereClause, [Map<String, dynamic>? whereValues,
      String? fromClause, String? shortcut, AccessOption? option]) async {
    Set<String>? fds;
    if (fields != null) {
      fds = LinkedHashSet<String>();
      fds..add(fdOid)..addAll(fields);
    }

    final entities = <T>[];
    await for (final row in
        queryFrom(fds, fromClause ?? newInstance('*').otype,
        whereClause, whereValues, shortcut, option)) {
      entities.add(toEntityNS(row, fields, newInstance));
    }
    return entities;
  }

  /// Instantiates an Entity instance to represent the data in [row].
  /// If [row] is null, this method will return `Future.value(null)`.
  T? toEntity<T extends Entity>(Row? row, Iterable<String>? fields,
      T newInstance(String oid))
  => row == null ? null: toEntityNS(row, fields, newInstance);

  /// Instantiates an Entity instance to represent the data in [row].
  T toEntityNS<T extends Entity>(Row row, Iterable<String>? fields,
      T newInstance(String oid)) {
    final data = HashMap<String, dynamic>();
    row.forEach((String name, value) => data[name] = value);
    assert(data.containsKey(fdOid)); //fdOid is required.
    return bind_(this, data.remove(fdOid) as String, newInstance, data, fields);
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
  Future<T?> loadWith<T extends Entity>(
      Iterable<String>? fields, T newInstance(String oid),
      String? whereClause, [Map<String, dynamic>? whereValues,
      String? fromClause, String? shortcut, AccessOption? option]) async {
    Set<String>? fds;
    if (fields != null) {
      fds = LinkedHashSet<String>();
      fds..add(fdOid)..addAll(fields);
    }

    final row = await StreamUtil.first(queryFrom(fds,
        fromClause ?? newInstance('*').otype,
        _limit1(whereClause, selectRequired: false)
        ?? (fromClause != null && _reLimit.hasMatch(fromClause) ? null: 'limit 1'),
        whereValues, shortcut, option));
      return toEntity(row, fields, newInstance);
  }

  /** Loads all entities of the given AND criteria.
   * By AND, we mean it satisfies all values in [whereValues].
   * 
   * * [option] - whether to use [forShare], [forUpdate]
   * or null (default; no lock).
   */
  Future<List<T>> loadAllBy<T extends Entity>(
      Iterable<String>? fields, T newInstance(String oid),
      Map<String, dynamic> whereValues, [AccessOption? option])
  => loadAllWith(fields, newInstance,
      sqlWhereBy(whereValues), whereValues, null, null, option);

  /** Loads the first entity of the given AND criteria.
   * By AND, we mean it satisfies all values in [whereValues].
   * 
   * * [option] - whether to use [forShare], [forUpdate]
   * or null (default; no lock).
   */
  Future<T?> loadBy<T extends Entity>(
      Iterable<String>? fields, T newInstance(String oid),
      Map<String, dynamic> whereValues, [AccessOption? option])
  => loadWith(fields, newInstance,
      sqlWhereBy(whereValues, "limit 1"), whereValues, null, null, option);

  ///Deletes the entity of the given [oid].
  Future<int> delete(String otype, String oid) {
    uncache(otype, oid);
    return execute('delete from "$otype" where "$fdOid"=@$fdOid',
      {fdOid: oid});
  }

  /// Tests if the given [oid] exists.
  ///
  /// * [fromClause] - any valid from clause, such as a table name,
  /// an inner join, and so on.
  /// Note: it shall not include `from`.
  /// Example: `Foo`, `"Foo" inner join "Moo" on ref=oid`,
  /// and `"Foo" F`.
  Future<bool> exists(String fromClause, String oid) async
  => null != await queryAnyBy(const [], fromClause, {fdOid: oid});

  /// Inserts the entity specified in data.
  /// Note: all fields found in [data] are written. You have to
  /// remove unnecessary files by yourself, such as [fdOtype].
  /// 
  /// * [types] - a map of (field-name, field-type). If specified,
  /// the type of the field will be retrieved from [types], if any.
  /// * [append] - the extra clause to append to the insert statement.
  /// Example, `final oid = await insert(..., append: returning "$fdOid");`
  Future<dynamic> insert(String otype, Map<String, dynamic> data,
      {Map<String, String>? types, String? append}) {
    final sql = StringBuffer('insert into "')..write(otype)..write('"('),
      values = StringBuffer(" values("),
      cvter = _pool!.typeConverter;

    bool first = true;
    data.forEach((fd, val) {
      if (first) first = false;
      else {
        sql.write(',');
        values.write(',');
      }
      sql..write('"')..write(fd)..write('"');
      values.write(cvter.encode(val, types?[fd]));
    });

    sql.write(')');
    values.write(')');
    bool bReturning = false;
    if (append != null) {
      bReturning = append.trim().startsWith('returning');
      values..write(' ')..write(append);
    }

    sql.write(values);
    final stmt = sql.toString();
    if (bReturning)
      return query(stmt, data).first.then(_firstCol);

    return execute(stmt, data);
  }
  static _firstCol(Row row) => row[0];

  //Begins a transaction
  Future<int> _begin() => execute('begin');
  //Commits
  Future<int> _commit() => execute('commit');
  //Rollback
  Future<int> _rollback() => execute('rollback');

  Future<int> _rollbackSafely()
  => _rollback()
    .timeout(const Duration(seconds: 15), onTimeout: _asZero) //simply ignore
    .catchError(_asZero);

  static int _asZero([Object? ex]) => 0;
}
