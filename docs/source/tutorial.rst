..  spinedb_api tutorial
    Created: 18.6.2018

.. _SQLAlchemy: http://www.sqlalchemy.org/


********
Tutorial
********

Spine database API provides for the creation and management of
Spine database object relational mappings, using SQLAlchemy_ as the underlying engine.
This tutorial will provide a full introduction to the usage of this package.

To begin, make sure Spine database API is installed as described at :ref:`installation`.



.. contents::
   :local:


Mapping
-------

Usage of Spine database API starts with the creation of a *Database Mapping*. This is
a Python object that provides simple access to a Spine database.
Depending on your purpose, you may chose between two classes of mapping:

- If you just want to *query* the database (i.e., run ``SELECT`` statements) but don't do any
  modifications, you should use the :class:`.DatabaseMapping` class.
- If you want to query the database but also *modify* it, you should use the :class:`.DiffDatabaseMapping` class.

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



Custom querying
===============



Inserting
---------

  operate on the mapping, and subsequently on the
  database: query it, add to it, update it, remove from it, commit and rollback the changes.
  To create mapping to a Spine database, you just need to know the URL
