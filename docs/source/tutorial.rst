..  spinedb_api tutorial
    Created: 18.6.2018

.. _SQLAlchemy: http://www.sqlalchemy.org/


********
Tutorial
********

Spine database API provides for the creation and management of
Spine databases, using SQLAlchemy_ as the underlying engine.
This tutorial will provide a full introduction to the usage of this package.

To begin, make sure Spine database API is installed as described at :ref:`installation`.


Creation
--------

Usage of Spine database API starts with the creation of a Spine database.

Mapping
-------

Next step is the creation of a *Database Mapping*,
a Python object that provides means of interacting with the database.
Spine database API provides two classes of mapping:

- :class:`.DatabaseMapping`, just for *querying* the database (i.e., run ``SELECT`` statements).
- :class:`.DiffDatabaseMapping`, for both querying and *modifying* the database.

The differences between these two will become more apparent as we go through this tutorial.
However, it is important to note that everything you can do with a :class:`.DatabaseMapping`,
you can also do with a :class:`.DiffDatabaseMapping`.

To create a :class:`.DatabaseMapping`, we just pass the database URL to the class constructor::

    from spinedb_api import DatabaseMapping

    url = "sqlite:///spine.db"

    db_map = DatabaseMapping(url)

The URL should be formatted following the RFC-1738 standard, so it basically
works with :func:`sqlalchemy.create_engine` as described
`here <https://docs.sqlalchemy.org/en/13/core/engines.html?highlight=database%20urls#database-urls>`_.

.. note::

  Currently supported database backends are only SQLite and MySQL. More will be added later.

Querying
--------

The database mapping object provides two mechanisms for querying the database.
The first is for running *standard*, general-purpose queries
such as selecting all records from the ``object_class`` table.
The second is for performing *custom* queries that one may need for a particular purpose.

Standard querying
=================

To perform standard querying, we chose among the methods of the :class:`~.DatabaseMappingQueryMixin` class,
the one that bets suits our purpose. E.g.::

    TODO

Custom querying
===============

TODO

Inserting
---------

TODO
