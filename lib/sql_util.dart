//Copyright (C) 2024 Potix Corporation. All Rights Reserved.
//History: Sun May 12 20:11:52 CST 2024
// Author: tomyeh
library access.sql_util;

import "package:charcode/ascii.dart";
import "package:postgresql2/postgresql.dart";
import "package:rikulo_commons/util.dart";

/// Encodes [text] to escape special characters, so it can be *normal* text
/// when using as part of the LIKE clause.
/// 
/// By *normal* we mean the text matches *exactly* character-by-character.
/// Any special characters in [text], such as '%' and '_', will be encoded,
/// so `%` matches `%`, no longer wildcard.
/// 
/// NOTE: the escape character is `!`, so you must append
/// `escape '!'` at the end. For example,
/// 
///     "where abc like E'${encodeTextInLike(input)}%' escape '!'"
///
/// >Useful for mixing user's input with the LIKE patterns.
String encodeTextInLike(String text) => _encLikeStr.apply(text);
const _encLikeMap = const {...escapes,
    '%': r'!%', '_': r'!_', '!': '!!'};
final _encLikeStr = ReplaceAll(_encLikeMap);
    //Implementation Note: we CANNOT use `escape '\'` since E'' is used

/// Encodes [text] to escape special characters, so it can be *normal* text
/// when using as part of the regular-expression clause (`~` and `~*).
/// 
/// By *normal* we mean the text matches *exactly* character-by-character.
/// Any special characters in [text], such as '.' and '*', will be encoded,
/// so `.` matches `.`, no longer wildcard.
/// 
/// Example: `"name" ~* E'[A-Z]+${encodeRegexp(input)}'
String encodeTextInRegex(String text)
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
