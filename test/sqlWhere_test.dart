//Copyright (C) 2018 Potix Corporation. All Rights Reserved.
//History: Tue Aug  7 14:34:39 CST 2018
// Author: tomyeh
library sqlColumns_test;

import 'package:test/test.dart';
import 'package:access/access.dart';

void main() {
  test("sqlWhereBy", () {
    expect(sqlWhereBy({
      "a": null,
      "b": not(null),
      "c": 12,
      "d": not("abc"),
      }), '"a" is null and "b" is not null and "c"=@c '
          'and "d"!=@d');
  });
}
