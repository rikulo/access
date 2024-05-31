//Copyright (C) 2024 Potix Corporation. All Rights Reserved.
//History: Sun May 12 21:20:20 CST 2024
// Author: tomyeh
part of access;

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
  pgCheckViolation = "23514",
  pgInvalidRegex = "2201B",
  pgProgramLimitExceeded = "54000",
  pgOutOfMemory = '53200';

/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate a field shall not be the given value,
/// so-called negative condition.
/// 
/// Example,
/// 
///     await access.queryBy(..., {"removedAt": notNull, "type": not(1)});
///
/// > See also [inList] and [notNull]
NotCondition<T> not<T>(T value) => NotCondition<T>(value);

/// Used in the `whereValues` to represent a not-null condition.
const notNull = const NotCondition(null);

/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate an IN clause.
/// That is, it shall be generated with `field in (value1, value2)`
/// 
/// Example,
///
///     await access.queryBy(..., {"users": inList(['john', 'mary'])});
///
/// > See also [not] and [notIn].
InCondition inList(Iterable? value) => InCondition(value);
/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate a NOT IN clause.
///
/// > See also [inList] and [not].
NotCondition<InCondition> notIn(Iterable? value)
=> NotCondition(InCondition(value));

/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate a LIKE clause.
/// That is, it shall be generated with `field in (value1, value2)`
/// 
/// Example,
///
///     await access.queryBy(..., {"name": like('a%z')});
///
/// * [escape] the escape character. Specify it as `'!'` if
/// a portion of [pattern] has been encoded with [encodeTextInLike].
/// For example,
/// 
///     await access.queryBy(...,
///       {"name": like('${encodeTextInLike(text)}%'), '!')})
/// 
/// Assume text is 'a%b', then it generates
/// 
///     ...like 'a!%b%' escape '!'
///
/// Also note, if [escape] is specified, [pattern] is assumed to be
/// encoded properly. That is, [sqlWhereBy] won't encode it again.
/// See #10.
/// 
/// > See also [not] and [notIn].
LikeCondition like(String pattern, [String? escape])
=> LikeCondition(pattern, escape);
/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate a NOT Like clause.
///
/// > See also [like], [inList] and [not].
NotCondition<LikeCondition> notLike(String pattern, [String? escape])
=> NotCondition(LikeCondition(pattern, escape));

/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate a negative condition.
/// 
/// In most cases, you shall use [not] instead for its simplicity.
/// 
/// Use [Not] only for constructing a constant conditions:
///
///     const {
///       "foo": const Not(null),
///       "key": const Not("abc"),
///     }
class NotCondition<T> {
  final T value;
  const NotCondition(T this.value);

  @override
  String toString() => 'not($value)';
}

/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate an IN clause.
class InCondition {
  final Iterable? value;
  InCondition(this.value);

  @override
  String toString() => 'in($value)';
}

/// Used in the `whereValues` of [DBAccess.loadBy], [DBAccess.queryBy],
/// and [sqlWhereBy] to indicate a Like clause.
class LikeCondition {
  final String pattern;
  final String? escape;
  LikeCondition(this.pattern, [this.escape]);

  @override
  String toString() => 'like($pattern, $escape)';
}

///Whether it is [PostgresqlException] about the violation of the given [code].
bool isViolation(ex, String code)
=> ex is PostgresqlException && ex.serverMessage?.code == code;

///Whether it is [PostgresqlException] about the violation of uniqueness.
///It is useful with select-for-update
bool isUniqueViolation(ex) => isViolation(ex, pgUniqueViolation);
///Whether it is [PostgresqlException] about the violation of foreign keys.
bool isForeignKeyViolation(ex) => isViolation(ex, pgForeignKeyViolation);
///Whether it is [PostgresqlException] about the violation of foreign keys.
bool isNotNullViolation(ex) => isViolation(ex, pgNotNullViolation);

///Collects the first column of [Row] into a list.
List firstColumns(Iterable<Row> rows) {
  final result = [];
  for (final row in rows)
    result.add(row[0]);
  return result;
}

/// Converts a list of [fields] to a SQL fragment separated by comma.
/// 
/// Note: if [fields] is null, `"*"` is returned, i.e., all fields are assumed.
/// if [fields] is empty, `1` is returned (so it is easier to construct 
/// a SQL statement).
/// 
/// Each field will be enclosed with a pair of double quotations, such as
/// `foo` => `"foo"`.
/// However, if it starts with a number or contains `(` or `"`,
/// it'll be output directly. In other words, it is considered as an expression.
/// 
/// For example, you can pass a field as
/// `("assignee" is not null or "due" is null)`.
/// Furthermore, you can name it with an alias:
///  `("assignee" is not null or "due" is null) alive`
/// 
/// Here is another example:
/// 
///     access.query('select ${sqlColumns(fields)} from "Foo"');
/// 
/// * [shortcut] - the table shortcut to prefix the field (column name).
/// If specified, the result will be `T."field1",T."field2"` if [shortcut] is `T`.
/// Note: [shortcut] is case insensitive.
String sqlColumns(Iterable<String>? fields, [String? shortcut]) {
  if (fields == null) return "*";
  if (fields.isEmpty) return '1';

  final sql = StringBuffer();
  addSqlColumns(sql, fields, shortcut);
  return sql.toString();
}

/// Adds a list of [fields] into [sql] by separating them with comma
/// See also [sqlColumns].
void addSqlColumns(StringBuffer sql, Iterable<String>? fields, [String? shortcut]) {
  assert(shortcut == null || !shortcut.contains(' '));

  if (fields == null) {
    sql.write("*");
    return;
  }
  if (fields.isEmpty) {
    sql.write('1');
    return;
  }

  assert(fields is Set || fields.toSet().length == fields.length, "Dup? $fields");

  bool first = true;
  for (final field in fields) {
    if (first) first = false;
    else sql.write(',');
    _appendField(sql, field, shortcut);
  }
}

void _appendField(StringBuffer sql, String field, [String? shortcut]) {
  if (_reExpr.hasMatch(field)) {
    sql.write(field);
  } else {
    if (shortcut != null)
      sql..write(shortcut)..write('.');
    sql..write('"')..write(field)..write('"');
  }
}
final _reExpr = RegExp(r'(?:^[0-9]|[("+|])');

/// Returns the where criteria (without where) by concatenating all values
/// found in [whereValues] with *and*.
/// 
/// Each key will be enclosed with a pair of double quotations, such as
/// `foo` => `"foo"`.
/// However, if it starts with a number or contains `(` or `"`,
/// it'll be output directly. In other words, it is considered as an expression.
/// 
/// If a value in [whereValues] is null, it will generate
/// `"name" is null`.
/// Furthermore, you can use [inList], [notIn], and [notNull]
/// to generate more sophisticated conditions. For example,
///
///     {
///       "foo": inList(['a', 'b']),
///       "moo": notIn([1, 5]),
///       "boo": notNull,
///       "qoo": null,
///       "xoo": not(90),
///     }
/// 
/// Furthermore, you can put the order-by and limit clause in the key
/// with empty value. For example,
///
///     {
///       "foo": foo,
///       "": 'order by value desc limt 5',
///     }
String sqlWhereBy(Map<String, dynamic> whereValues, [String? append]) {
  final cvter = _pool!.typeConverter,
    sql = StringBuffer();
  var first = true;
  whereValues.forEach((name, value) {
    if (name.isEmpty) { //value is a SQL fragment to append
      assert(value is String);
      append = append == null ? value.toString(): '$value $append';
      return;
    }

    if (first) first = false;
    else sql.write(' and ');

    bool negate;
    if (negate = value is NotCondition) value = value.value;

    if (value is InCondition) {
      value = value.value;
      if (value == null || value.isEmpty) {
        sql.write(negate ? 'true': 'false');
        return;
      }

      _appendField(sql, name);
      if (negate) sql.write(' not');
      sql.write(' in (');

      var first = true;
      for (final item in value) {
        if (first) first = false;
        else sql.write(',');
        sql.write(cvter.encode(item, null));
      }
      sql.write(')');
      return;
    }

    _appendField(sql, name);
    if (value is LikeCondition) {
      if (negate) sql.write(' not');
      sql.write(' like ');

      //#10: don't encode again if escape is specified
      //i.e., assume pattern is encoded properly.
      final escape = value.escape;
      if (escape != null)
        sql..write("E'")..write(value.pattern)
          ..write("' escape '")..write(escape)..write("'");
      else
        sql.write(cvter.encode(value.pattern, null));

    } else if (value != null) {
      if (negate) sql.write('!');
      sql..write('=')..write(cvter.encode(value, null));

    } else {
      sql.write(' is ');
      if (negate) sql.write("not ");
      sql.write('null');
    }
  });

  if (append != null) sql..write(' ')..write(append);
  return sql.toString();
}

/// A callback, if specified, is called before starting an access
/// (aka., a transaction).
///
/// - [accessCount] number of transactions before starting a transaction
/// for [command].
void Function(FutureOr Function(DBAccess access) command, int accessCount)?
  onAccess;
  // The signature can't return FutureOr => O/W, we have to use:
  //  await onAccess?.call(...);
  // Then, it means `++_nAccess` won't be called immediately.
  // It means checking [accessCount] before calling [access] won't be reliable.

/// Put "limit 1" into [sql] if not there.
String? _limit1(String? sql)
=> sql == null ? null: _limit1NS(sql);

String _limit1NS(String sql)
=> !_reSelect.hasMatch(sql) || _reLimit.hasMatch(sql) ? sql: '$sql limit 1';
final _reLimit = RegExp(r'(?:\slimit\s|;)', caseSensitive: false),
  _reSelect = RegExp(r'^\s*select\s', caseSensitive: false);

Future _invokeTask(FutureOr task()) async {
  try {
    await task();
  } catch (ex, st) {
    _logger.severe("Failed to invoke $task", ex, st);
  }
}

Future _invokeTaskWith<T>(FutureOr task(T arg), T arg) async {
  try {
    await task(arg);
  } catch (ex, st) {
    _logger.severe("Failed to invoke $task with $arg", ex, st);
  }
}
