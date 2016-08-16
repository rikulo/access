# Access

A simple database utility for the [entity](https://github.com/rikulo/entity) library.

* [API Reference](http://www.dartdocs.org/documentation/access/0.9.2)

[![Build Status](https://drone.io/github.com/rikulo/access/status.png)](https://drone.io/github.com/rikulo/access/latest)

## Use

    access((DBAccess access) async {
      final rows = await access.query('select ...').toList();
      ...
      await access.execute('update...');
    })
    //transactions ends here; roll back if an uncaught exception is thrown
    .catchError((ex, st) {
      ...
    });

## Who Uses

* [Quire](https://quire.io) - a simple, collaborative, multi-level task management tool.
