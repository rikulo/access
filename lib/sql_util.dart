//Copyright (C) 2024 Potix Corporation. All Rights Reserved.
//History: Sun May 12 20:11:52 CST 2024
// Author: tomyeh
library access.sql_util;

import "package:charcode/ascii.dart";
import "package:postgresql2/postgresql.dart";
import "package:rikulo_commons/util.dart";

/// Encodes [text] so it can be used with '%' and '_' in a LIKE clause.
/// Note: any special characters including '%' and '_' will be encoded,
/// so you shall put them after calling this method.
/// 
/// Example, `"where abc like E'${DBUtil.encodeLike(pattern)}%' escape '!'"`.
/// 
/// Note:
/// 1. you have to put `E` before the string
/// 2. you have to put `escape '!'` after the string
String encodeLike(String text) => _encLikeStr.apply(text);
const _encLikeMap = const {...escapes,
    '%': r'!%', '_': r'!_', '!': '!!'};
final _encLikeStr = ReplaceAll(_encLikeMap);
    //Implementation Note: we CANNOT use `escape '\'` since E'' is used

/// Encodes [text] so it can be used in `~` or `~*` statement.
/// 
/// Example: `"name" ~* E'${DBUtil.encodeRegexp(text)}'
String encodeRegex(String text)
=> text.replaceAllMapped(_reRegex, _encRegex);
String _encRegex(Match m) {
  final cc = m[0]!,
    code = cc.codeUnitAt(0),
    c2 = escapes[cc];
  return c2 == null ? r"\\" "$cc": //part of E'xxx', so use \\ for backslash
      code == $backslash ? r'\\\\': //#11535: 1) E'xxx', \\\\ => \\, 2) regex parsing: \\ => \
      code < $space && code > 0 ? "\\$c2": c2; //#11535 '\n' => r'\\n' => same reason as above
}
final _reRegex = RegExp(r"[\].*+?[()^${" "$escapePattern]");
  //unlike other regex, in Dart, we have to specify `\]` instead of
  //putting as the first. Also, specify `\\`.
