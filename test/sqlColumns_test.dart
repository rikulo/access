//Copyright (C) 2018 Potix Corporation. All Rights Reserved.
//History: Tue Aug  7 14:34:39 CST 2018
// Author: tomyeh
library sqlColumns_test;

import 'package:test/test.dart';
import 'package:access/access.dart';

void main() {
  test("sqlColumns", () {
    final cases = <(List<String>?, String)>[
      (['fA', 'fB'], '"fA","fB"'),
      (['1', 'max("foo")'], '1,max("foo")'),
      (['f1', 'f1+f2'], '"f1",f1+f2'),
      ([], '1'),
      (null, '*'),
      (['*'], '*'),
      (['count(*)'], 'count(*)'),
      (['distinct "user"'], 'distinct "user"'),
    ];

    for (final (cols, result) in cases)
      expect(sqlColumns(cols), result);
  });
}
