# Access

A simple database utility for the [entity](https://github.com/rikulo/entity) library.

* [API Reference](http://www.dartdocs.org/documentation/access/1.0.3+1)

[![Build Status](https://drone.io/github.com/rikulo/access/status.png)](https://drone.io/github.com/rikulo/access/latest)

## Use

    access((DBAccess access) async {
      await for (final Row row in access.query('select ...')) {
        ...
      }
      ...
      await access.execute('update...');
    })
    //transactions ends here; roll back if an uncaught exception is thrown
    .catchError((ex, st) {
      ...
    });

## Who Uses

* [Quire](https://quire.io) - a simple, collaborative, multi-level task management tool.
* [Keikai](https://keikai.io) - a sophisticated spreadsheet for big data
