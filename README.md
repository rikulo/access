# Access

A simple database utility for the [entity](https://github.com/rikulo/entity) library.

* [API Reference](https://pub.dev/documentation/access/latest/)
* [Github Repos](https://github.com/rikulo/access)

## Use

    await access((access) async {
      await for (final row in access.query('select ...')) {
        ...
      }
      ...
      await access.execute('update...');
    });
    //transactions ends here; roll back if an uncaught exception is thrown

## Who Uses

* [Quire](https://quire.io) - a simple, collaborative, multi-level task management tool.
* [Keikai](https://keikai.io) - a sophisticated spreadsheet for big data
