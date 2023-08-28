//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Fri, Jul 04, 2014  1:58:32 PM
// Author: tomyeh
part of access.dbtool;

///Purges all data and schemas.
Future purge(Connection conn,
    Map<String, Map<String, SqlType>> tables,
    Map<String, IndexInfo> indexes, Map<String, RuleInfo> rules) {

  return Future.forEach(rules.keys, (name) async {
    try {
      return await conn.execute('drop rule "$name" on "${rules[name]!.table}"');
    } catch (ex) {
      if (!_isUndefined(ex)) rethrow;
    }
  })
  .then((_) => Future.forEach(indexes.keys.toList().reversed, (name) async {
    try {
      return await conn.execute('drop index "$name"');
    } catch (ex) {
      if (!_isUndefined(ex)) rethrow;
    }
  }))
  .then((_) {
    final tblsGened = HashSet<String>();
    final refsDeferred = <_DeferredRef>[];
    for (final otype in tables.keys)
      _scanDeferredRefs(otype, tables[otype]!, tblsGened, refsDeferred);
    return Future.forEach(refsDeferred,
        (_DeferredRef defRef) => defRef.drop(conn)
        .catchError((ex) {}, test: _isUndefined));
  })
  .then((_) => Future.forEach(tables.keys.toList().reversed, (name) async {
    try {
      return await conn.execute('drop table "$name"');
    } catch (ex) {
      if (!_isUndefined(ex)) rethrow;
    }
  }));
}

void _scanDeferredRefs(String otype, Map<String, SqlType> table,
    Set<String> tblsGened, List<_DeferredRef> refsDeferred) {
  tblsGened.add(otype);

  for (final String col in table.keys) {
    if (col.startsWith(copy)) {
      _scanDeferredRefs(otype, (table[col] as CopyType).source,
        tblsGened, refsDeferred);
      continue;
    }

    final refType = table[col];
    if (refType is ReferenceType) {
      if (!tblsGened.contains(refType.otype)) //deferred
        refsDeferred.add(
          _DeferredRef(refType.otype, refType.column,
              otype, col, refType.cascade));
    }
  }
}

bool _isUndefined(ex)
=> isViolation(ex, pgUndefinedObject) || isViolation(ex, pgUndefinedTable);
