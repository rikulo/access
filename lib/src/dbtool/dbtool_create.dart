//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Thu, Jul 03, 2014  6:38:28 PM
// Author: tomyeh
part of access.dbtool;

///Creates the tables specified in version.
Future create(Connection conn,
    double version, Map<String, Map<String, SqlType>> tables,
    Map<String, IndexInfo> indexes, Map<String, RuleInfo> rules,
    String script) {
  final Set<String> tblsGened = new HashSet();
  final List<_DeferredRef> refsDeferred = [];

  return Future.forEach(tables.keys,
    (String otype) =>
      _createTable(conn, otype, tables[otype], tblsGened, refsDeferred))
  .then((_) => Future.forEach(refsDeferred,
    (_DeferredRef defRef) => defRef.create(conn)))
  .then((_) => Future.forEach(indexes.keys,
    (String name) => _createIndex(conn, name, indexes[name])))
  .then((_) => Future.forEach(rules.keys,
    (String name) => _createRule(conn, name, rules[name])))
  .then((_) {
    if (script != null)
      return conn.execute(script);
   });
}

Future _createTable(Connection conn, String otype, Map<String, SqlType> table,
      Set<String> tblsGened, List<_DeferredRef> refsDeferred) {
  print("Creating table: $otype...");
  tblsGened.add(otype);

  final List<String> query = ['create table "', otype, '" (\n'];
  _genCreateColumns(query, otype, table, tblsGened, refsDeferred, true);
  query.add(')');
  return conn.execute(query.join(''));
}

class _DeferredRef {
  final String otypePrimary;
  final String otypeForeign;
  final String columnForeign;
  final String cascade;

  _DeferredRef(this.otypePrimary, this.otypeForeign, this.columnForeign,
    this.cascade);

  Future create(Connection conn)
  => conn.execute(['alter table "', otypeForeign,
      '" add constraint "fk_', columnForeign,
      '" foreign key("', columnForeign, '") references "',
      otypePrimary, '"("', F_OID, '") ', cascade].join(''));
  Future drop(Connection conn)
  => conn.execute(['alter table "', otypeForeign,
      '" drop constraint "fk_', columnForeign, '"'].join(''));
}

bool _genCreateColumns(List<String> query, String otype,
      Map<String, SqlType> table, Set<String> tblsGened,
      List<_DeferredRef> refsDeferred, bool first) {
  for (final String col in table.keys) {
    if (col.startsWith(COPY)) {
      first = _genCreateColumns(query, otype,
          (table[col] as CopyType).source, tblsGened, refsDeferred, first);
      continue;
    }

    if (first) first = false;
    else query.add(',\n');

    final SqlType sqlType = table[col];

    if (col.startsWith(DEFINE)) {
      query.add(sqlType.toSqlString());
      continue;
    }

    query..add('"')..add(col)..add('" ');
    if (sqlType is ReferenceType) {
      final ReferenceType refType = sqlType;
      final bool deferred = !tblsGened.contains(refType.otype);
      query.add(refType.toSqlStringBy(deferred));

      if (deferred)
        refsDeferred.add(
          new _DeferredRef(refType.otype, otype, col, refType.cascade));
    } else {
      query.add(sqlType.toSqlString());
    }
  }
  return first;
}

Future _createIndex(Connection conn, String name, IndexInfo info) {
  print("Creating index: $name...");

  final List<String> query = ['create '];
  if (info.unique)
    query.add('unique ');
  query..add('index "')..add(name)..add('" on "')
    ..add(info.table)..add('"(');

  bool first = true;
  for (final String col in info.columns) {
    if (first) first = false;
    else query.add(',');
    query..add('"')..add(col)..add('"');
  }
  query.add(')');

  return conn.execute(query.join(''));
}

Future _createRule(Connection conn, String name, RuleInfo info) {
  print("Creating rule: $name...");
  return conn.execute('create rule "$name" as ${info.rule}');
}
