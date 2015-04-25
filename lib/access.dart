//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Thu, Jul 03, 2014  4:43:13 PM
// Author: tomyeh
library access;

import "dart:async";
import "dart:convert" show JSON;
import "dart:collection" show HashSet, HashMap;

import "package:logging/logging.dart" show Logger;
import "package:postgresql2/postgresql.dart";
import "package:postgresql2/postgresql_pool.dart";
import "package:entity/postgresql2.dart";
import "package:entity/entity.dart";
import "package:rikulo_commons/util.dart";

export "package:postgresql2/postgresql.dart"
  show Connection, PgServerException, Row;

final Logger _logger = new Logger("access");

const String
  PG_DUPLICATE_TABLE = "42P07",
  PG_FAILED_IN_TRANSACTION = "25P02",
  PG_UNDEFINED_OBJECT = "42704",
  PG_UNDEFINED_TABLE = "42P01",
  PG_UNIQUE_VIOLATION = "23505";

///Whether it is [PgServerException] about the violation of uniqueness.
///It is useful with select-for-update
bool isUniqueViolation(ex)
=> ex is PgServerException && ex.code == PG_UNIQUE_VIOLATION;

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
  bool _closed = false;

  DBAccess(Connection conn): super(conn, cache: true);

  /** A map of application-specific data.
   */
  Map<String, dynamic> get dataset
  => _dataset != null ? _dataset: MapUtil.auto(() => _dataset = new HashMap());

  /// Queues a command for execution, and when done, returns the number of rows
  /// affected by the SQL command.
  Future<int> execute(String sql, [values]) {
    assert(!_closed);
    return conn.execute(sql, values)
    .catchError((ex, st) {
      _logger.warning("Failed execute($sql, $values)", ex, st);
      return new Future.error(ex, st);
    });
  }

  /// Queue a SQL query to be run, returning a [Stream] of rows.
  Stream<Row> query(String sql, [values]) {
    assert(!_closed);
    return conn.query(sql, values);
  }

  ///Returns the first result, or null if not found.
  Future<Row> queryAny(String sql, [values])
  => query(sql, values).first
    .catchError((ex) => null, test: (ex) => ex is StateError)
    .catchError((ex, st) {
      _logger.warning("Failed query($sql, $values)", ex, st);
      return new Future.error(ex, st);
    });

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
    .catchError((ex) => null, test: (ex) => ex is StateError)
    .catchError((ex, st) {
      _logger.warning("Failed queryBy($fields, $otype, $whereValues)", ex, st);
      return new Future.error(ex, st);
    });

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
    String sql = 'select ${_sqlCols(fields)} from "$otype"';
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
    .catchError((ex) => null, test: (ex) => ex is StateError)
    .catchError((ex, st) {
      _logger.warning("Failed queryWith($fields, $otype, $whereClause)", ex, st);
      return new Future.error(ex, st);
    });

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
    .catchError((ex, st) {
      _logger.warning("Failed loadAllWith($fields, $whereClause, $whereValues)", ex, st);
      return new Future.error(ex, st);
    })
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
    .catchError((ex, st) {
      _logger.warning("Failed loadWith($fields, $whereClause)", ex, st);
      return new Future.error(ex, st);
    })
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

///Converts a list of [fields] to a SQL fragment separated by comma.
String _sqlCols(Iterable<String> fields) {
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

/** Initializes the database access.
 *
 * > Note: it must be initialized before accessing any other methods.
 */
Future initAccess(String dbUri, {int poolSize: 200}) {
  endAccess();

  _initFormatValue();
  _pool = new Pool(dbUri, min: 10, max: poolSize);
  return _pool.start();
}
/** Cleans up.
 *
 * To stop accessing, you can invoke this to return the connections
 * being pooled.
 */
void endAccess() {
  if (_pool != null) {
    extendedFormatValue = _prevExtendedFormatValue;
    final Pool pool = _pool;
    _pool = null;
    pool.destroy();
  }
}
Pool _pool;

void _initFormatValue() {
  _prevExtendedFormatValue = extendedFormatValue;

  extendedFormatValue = (value, String type, formatString(String s)) {
    if (type == null)
      return formatString(JSON.encode(value));
    throw new Exception("Unsupported type as query parameters: $value ($type).");
  };
}
Function _prevExtendedFormatValue;
