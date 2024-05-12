//Copyright (C) 2018 Potix Corporation. All Rights Reserved.
//History: Tue Aug  7 14:34:39 CST 2018
// Author: tomyeh
library sqlColumns_test;

import 'package:test/test.dart';
import 'package:access/access.dart';
import 'package:postgresql2/pool.dart';

void main() {
  //for testing purpose, we have to configure it to prepare the type converter
  configure(Pool('dummy'));

  test("sqlWhereBy", () {
    expect(sqlWhereBy({
      "a": null,
      "b": not(null),
      "c": 12,
      "d": not("abc"),
      "e": notNull,
      }), '''
"a" is null and "b" is not null and "c"=12 and "d"!= E'abc'  and "e" is not null''');

    expect(sqlWhereBy({
      'foo': inList([1, 2, 3]),
      'f2': notIn([5, 6]),
      'f3': inList([]),
      'f4': notIn([]),
      }), '''
"foo" in (1,2,3) and "f2" not in (5,6) and false and true''');
  });
}
