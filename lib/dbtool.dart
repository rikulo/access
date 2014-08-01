//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Thu, Jul 03, 2014  3:59:29 PM
// Author: tomyeh
library access.dbtool;

import "dart:async";
import "dart:collection" show HashSet;

import "package:postgresql2/postgresql.dart";
import "package:entity/entity.dart" show F_OID;

import "access.dart";

part "src/dbtool/dbtool_create.dart";
part "src/dbtool/dbtool_purge.dart";

const String NOT_NULL = "not null", NULL = "null",
  ON_DELETE_CASCADE = "on delete cascade",
  ON_DELETE_SET_NULL = "on delete set null",
  COPY = ".copy.", COPY_1 = "$COPY.1", COPY_2 = "$COPY.2";

///Represents a SQL type
abstract class SqlType {
  factory SqlType(String type, [String constraint=NOT_NULL])
  => new _SqlType(type, constraint);

  ///Returns the string that will be part of SQL statement.
  String toSqlString();
  List toJson();
}

abstract class ReferenceType extends SqlType {
  factory ReferenceType(String otype, [String constraint=NOT_NULL,
    String cascade=""])
  => new _RefType(otype, constraint, cascade);

  ///Returns the SQL statment depending on if the reference is deferred.
  ///By default, we mean the referenced table is not created yet.
  String toSqlStringBy(bool deferred);

  final String cascade;

  ///Returns the name of the table having the primary key.
  String get otype;
}

///A special type used with [COPY] to copy fields from another map
class CopyType implements SqlType {
  final Map<String, SqlType> source;

  CopyType(Map<String, SqlType> this.source);

  @override
  String toSqlString() => throw new UnsupportedError(toString());
  @override
  List toJson() => throw new UnsupportedError(toString());
}

///Information of an index
class IndexInfo {
  final bool unique;
  final String table;
  final List<String> columns;

  IndexInfo(String this.table, List<String> this.columns, {bool this.unique:false});

  List toJson() => [table, columns, unique];
}

///The rule info
class RuleInfo {
  //The table that the rule is applied to.
  final String table;
  final String rule;

  RuleInfo(String this.table, String this.rule);
}

IndexInfo Index(String table, List<String> columns, {bool unique:false})
=> new IndexInfo(table, columns, unique: unique);
RuleInfo Rule(String table, String rule)
=> new RuleInfo(table, rule);

SqlType Text([String constraint=NOT_NULL])
=> new SqlType("text", constraint);

SqlType Char(int length, [String constraint=NOT_NULL])
=> new SqlType("char($length)", constraint);

SqlType Timestamptz([String constraint=NOT_NULL])
=> new SqlType("timestamptz(3)", constraint);

SqlType Integer([String constraint=NOT_NULL])
=> new SqlType("integer", constraint);

SqlType Smallint([String constraint=NOT_NULL])
=> new SqlType("smallint", constraint);

SqlType Double([String constraint=NOT_NULL])
=> new SqlType("double precision", constraint);

SqlType Boolean([String constraint=NOT_NULL])
=> new SqlType("boolean", constraint);

SqlType Json([String constraint=NOT_NULL])
=> new SqlType("json", constraint);

SqlType Reference(String otype, [String constraint=NOT_NULL, String cascade=""])
=> new ReferenceType(otype, constraint, cascade);

///A reference that refers to a record from two ore more different tables.
SqlType UnboundReference([String constraint=NOT_NULL])
=> new SqlType("text", constraint);

SqlType Oid()
=> new SqlType("text", 'not null primary key');

SqlType AutoOid() => new SqlType('bigserial', 'not null primary key');

SqlType Copy(Map<String, SqlType> source) => new CopyType(source);

class _SqlType implements SqlType {
  final String type;
  final String constraint;

  _SqlType(String this.type, String this.constraint);

  @override
  String toSqlString() => "$type $constraint";
  @override
  List toJson() => [type, constraint];
}

class _RefType implements ReferenceType {
  @override
  final String otype;
  final String constraint;
  @override
  final String cascade;

  _RefType(String this.otype, String this.constraint, String this.cascade);

  @override
  String toSqlStringBy(bool deferred)
  => deferred ? 'text $constraint':
    'text $constraint references "$otype"("$F_OID") $cascade';

  @override
  String toSqlString() => throw new UnsupportedError(toString());
  @override
  List toJson() => throw new UnsupportedError(toString());
}