//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Thu, Jul 03, 2014  6:38:28 PM
// Author: tomyeh
part of access.dbtool;

///Creates the tables specified in version.
Future create(Connection conn,
    double version, Map<String, Map<String, SqlType>> tables,
    Map<String, IndexInfo> indexes, Map<String, RuleInfo> rules) async {
  final Set<String> tblsGened = HashSet<String>();
  final List<_DeferredRef> refsDeferred = [];

  for (final otype in tables.keys)
    await _createTable(conn, otype, tables[otype]!, tblsGened, refsDeferred);
  for (final defRef in refsDeferred)
    await defRef.create(conn);

  for (final name in indexes.keys)
    await _createIndex(conn, name, indexes[name]!);
  for (final name in rules.keys)
    await _createRule(conn, name, rules[name]!);
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
  final String otypePrimary, columnPrimary;
  final String otypeForeign, columnForeign;
  final String cascade;

  _DeferredRef(this.otypePrimary, this.columnPrimary,
      this.otypeForeign, this.columnForeign, this.cascade);

  Future create(Connection conn)
  => conn.execute(['alter table "', otypeForeign,
      '" add constraint "fk_', columnForeign,
      '" foreign key("', columnForeign, '") references "',
      otypePrimary, '"("', columnPrimary, '") ', cascade].join(''));
  Future drop(Connection conn)
  => conn.execute(['alter table "', otypeForeign,
      '" drop constraint "fk_', columnForeign, '"'].join(''));
}

bool _genCreateColumns(List<String> query, String otype,
      Map<String, SqlType> table, Set<String> tblsGened,
      List<_DeferredRef> refsDeferred, bool first) {
  for (final String col in table.keys) {
    if (col.startsWith(copy)) {
      first = _genCreateColumns(query, otype,
          (table[col] as CopyType).source, tblsGened, refsDeferred, first);
      continue;
    }

    if (first) first = false;
    else query.add(',\n');

    final sqlType = table[col]!;

    if (col == primaryKey) {
      query..add('constraint "')..add(otype)..add('_pkey" primary key (');
      bool first = true;
      for (final col in (sqlType as PrimaryKeyType).columns) {
        if (first) first = false;
        else query.add(',');
        query..add('"')..add(col)..add('"');
      }
      query.add(')');
      continue;
    }

    if (col.startsWith(define)) {
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
          _DeferredRef(refType.otype, refType.column,
              otype, col, refType.cascade));
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
    ..add(info.table)..add('"');

  final using = info.using;
  if (using != null)
    query..add(' using ')..add(using)..add(' ');

  query.add('(');

  bool first = true;
  for (final String col in info.columns) {
    if (first) first = false;
    else query.add(',');
    query..add('"')..add(col)..add('"');
  }
  final ops = info.ops;
  if (ops != null)
    query..add(' ')..add(ops);
  query.add(')');

  final where = info.where;
  if (where != null)
    query..add(' where ')..add(where);

  return conn.execute(query.join(''));
}

Future _createRule(Connection conn, String name, RuleInfo info) {
  print("Creating rule: $name...");
  return conn.execute('create rule "$name" as ${info.rule}');
}
