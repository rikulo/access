//Copyright (C) 2014 Potix Corporation. All Rights Reserved.
//History: Thu, Jul 03, 2014  3:59:29 PM
// Author: tomyeh
library access.dbtool;

import "dart:async";
import "dart:collection" show HashSet;

import "package:postgresql2/postgresql.dart";
import "package:entity/entity.dart" show fdOid;

import "access.dart";

part "src/dbtool/dbtool_create.dart";
part "src/dbtool/dbtool_purge.dart";

const String notNull = "not null", nullable = "null",
  onDeleteCascade = "on delete cascade",
  onDeleteSetNull = "on delete set null";

/** Builtin name for special actions.
 */
const String
  /// Defines a primary key
  primaryKey = ".primary.",
  /// Copies content from another definition
  copy = ".copy.", copy1 = "$copy.1", copy2 = "$copy.2",
  define = ".define",  define1 = ".define.1",  define2 = ".define.2",
  define3 = ".define.3";

///Represents a SQL type
abstract class SqlType {
  factory SqlType(String type, {String constraint: notNull})
  => _SqlType(type, constraint: constraint);

  ///Returns the string that will be part of SQL statement.
  String toSqlString();
  List toJson();
}

abstract class ReferenceType extends SqlType {
  factory ReferenceType(String otype, {String constraint: notNull,
    String cascade = "", String column: fdOid})
  => _RefType(otype, constraint, cascade, column);

  ///Returns the SQL statment depending on if the reference is deferred.
  ///By default, we mean the referenced table is not created yet.
  String toSqlStringBy(bool deferred);

  String get cascade;
  String get column;

  ///Returns the name of the table having the primary key.
  String get otype;
}

/// A special type used with [primaryKey] to define a primary key
/// with multiple columns
class PrimaryKeyType implements SqlType {
  final Iterable<String> columns;

  PrimaryKeyType(this.columns);

  @override
  String toSqlString() => throw UnsupportedError(toString());
  @override
  List toJson() => throw UnsupportedError(toString());
}

///A special type used with [copy] to copy fields from another map
class CopyType implements SqlType {
  final Map<String, SqlType> source;

  CopyType(Map<String, SqlType> this.source);

  @override
  String toSqlString() => throw UnsupportedError(toString());
  @override
  List toJson() => throw UnsupportedError(toString());
}

///Information of an index
class IndexInfo {
  final bool unique;
  final String table;
  final List<String> columns;
  ///Types of index. Example, `gist` and `gin`.
  ///If null, default is assumed.
  final String? using;
  ///The operation class. Example: `jsonb_path_ops` and `varchar_pattern_ops`.
  ///If null, default is assumed.
  final String? ops;
  ///Conditional index
  final String? where;

  IndexInfo(String this.table, List<String> this.columns,
    {this.unique: false, this.using, this.ops, this.where});

  List toJson() => [table, columns, unique, using, ops, where];
}

///The rule info
class RuleInfo {
  //The table that the rule is applied to.
  final String table;
  final String rule;

  RuleInfo(String this.table, String this.rule);
}

IndexInfo Index(String table, List<String> columns,
    {bool unique: false, String? using, String? ops, String? where})
=> IndexInfo(table, columns, unique: unique, using: using, ops: ops,
    where: where);
RuleInfo Rule(String table, String rule)
=> RuleInfo(table, rule);

SqlType Text({String constraint: notNull})
=> SqlType("text", constraint: constraint);

SqlType Citext({String constraint: notNull})
=> SqlType("citext", constraint: constraint);

SqlType Char(int length, {String constraint: notNull})
=> SqlType("char($length)", constraint: constraint);

SqlType Timestamptz({String constraint: notNull})
=> SqlType("timestamptz(3)", constraint: constraint);

SqlType Integer({String constraint: notNull})
=> SqlType("integer", constraint: constraint);

SqlType Smallint({String constraint: notNull})
=> SqlType("smallint", constraint: constraint);

SqlType Bigint({String constraint: notNull})
=> SqlType("bigint", constraint: constraint);

SqlType Double({String constraint: notNull})
=> SqlType("double precision", constraint: constraint);

SqlType Real({String constraint: notNull})
=> SqlType("real", constraint: constraint);

SqlType Boolean({String constraint: notNull})
=> SqlType("boolean", constraint: constraint);

SqlType Serial({String constraint: notNull})
=> SqlType("serial", constraint: constraint);

SqlType Bigserial({String constraint: notNull})
=> SqlType("bigserial", constraint: constraint);

SqlType Json({String constraint: notNull})
=> SqlType("json", constraint: constraint);

SqlType Jsonb({String constraint: notNull})
=> SqlType("jsonb", constraint: constraint);

SqlType Tsvector({String constraint: notNull})
=> SqlType("tsvector", constraint: constraint);

/// A reference that refers to a record from another table.
/// It creates a foreign-key constraint to ensure the relationship.
SqlType Reference(String otype, {String constraint: notNull,
    String cascade = "", String column: fdOid})
=> ReferenceType(otype, constraint: constraint,
      cascade: cascade, column: column);

/// A multi-column primary key.
SqlType PrimaryKey(Iterable<String> columns) => PrimaryKeyType(columns);

/// A reference that refers to a record from another table.
///
/// Unlike [Reference], it doesn't create a constraint. Rather, it is
/// developer's job to ensure it.
/// It is useful if you prefer not to create an index, and can access with
/// other key(s).
/// 
/// * [otype] - it is useless but for documentation purpose
SqlType UnboundReference({String? otype, String constraint: notNull})
=> SqlType("text", constraint: constraint);

SqlType Oid()
=> SqlType("text", constraint: 'not null primary key');

SqlType AutoOid() => SqlType('bigserial', constraint: 'not null primary key');

/** Copyies the definition from another [source].
 * For example,
 *
 *     COPY: Copy(anotherTable),
 */
SqlType Copy(Map<String, SqlType> source) => CopyType(source);

/** Defines [definition], which is generated directly.
 * For example,
 *
 *     DEFINE: Define('primary key("column1", "column2")'),
 */
SqlType Define(String definition) => _DefineType(definition);

class _SqlType implements SqlType {
  final String type;
  final String constraint;

  _SqlType(String this.type, {String this.constraint: notNull});

  @override
  String toSqlString() => "$type $constraint";
  @override
  List toJson() => [type, constraint];
}

class _DefineType implements SqlType {
  final String definition;

  _DefineType(String this.definition);

  @override
  String toSqlString() => definition;
  @override
  List toJson() => [definition];
}

class _RefType implements ReferenceType {
  @override
  final String otype;
  final String constraint;
  @override
  final String column;
  @override
  final String cascade;

  _RefType(String this.otype, String this.constraint, String this.cascade,
      String this.column);

  @override
  String toSqlStringBy(bool deferred)
  => deferred ? 'text $constraint':
    'text $constraint references "$otype"("$column") $cascade';

  @override
  String toSqlString() => throw UnsupportedError(toString());
  @override
  List toJson() => throw UnsupportedError(toString());
}
