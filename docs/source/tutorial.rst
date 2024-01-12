..  spinedb_api tutorial
    Created: 18.6.2018

.. _SQLAlchemy: http://www.sqlalchemy.org/


********
Tutorial
********

The Spine DB API allows one to create and manipulate
Spine databases in a standard way, using SQLAlchemy_ as the underlying engine.
This tutorial provides a quick introduction to the usage of the package.

To begin, make sure Spine database API is installed as described in :ref:`installation`.


Database Mapping
----------------

The main mean of communication with a Spine DB is the :class:`.DatabaseMapping`,
specially designed to retrieve and modify data from the DB.
To create a :class:`.DatabaseMapping`, we just pass the URL of the DB to the class constructor::

    import spinedb_api as api
    from spinedb_api import DatabaseMapping

    url = "mysql://spine_db"  # The URL of an existing Spine DB

    with DatabaseMapping(url) as db_map:
        # Do something with db_map
        pass

The URL should be formatted following the RFC-1738 standard, as described
`here <https://docs.sqlalchemy.org/en/13/core/engines.html?highlight=database%20urls#database-urls>`_.

.. note::

  Currently supported database backends are SQLite and MySQL.

Creating a DB
-------------

If you're following this tutorial, chances are you don't have a Spine DB to play with just yet.
We can remediate this by creating a SQLite DB (which is just a file in your system), as follows::

    import spinedb_api as api
    from spinedb_api import DatabaseMapping

    url = "sqlite:///first.sqlite"

    with DatabaseMapping(url, create=True) as db_map:
        # Do something with db_map
        pass

The above will create a file called ``first.sqlite`` in your current working directoy.
Note that we pass the keyword argument ``create=True`` to :class:`.DatabaseMapping` to explicitly say
that we want the DB to be created at the given URL.

.. note::

  In the remainder we will skip the above step and work directly with ``db_map``. In other words,
  all the examples below assume we are inside the ``with`` block above.

Adding data
-----------

To insert data, we use e.g. :meth:`~.DatabaseMapping.add_entity_class_item`, :meth:`~.DatabaseMapping.add_entity_item`,
and so on.

Let's begin the party by adding a couple of entity classes::

    db_map.add_entity_class_item(name="fish", description="It swims.")
    db_map.add_entity_class_item(name="cat", description="Eats fish.")

Now let's add a multi-dimensional entity class between the two above. For this we need to specify the class names
as `dimension_name_list`::

    db_map.add_entity_class_item(
        name="fish__cat",
        dimension_name_list=("fish", "cat"),
        description="A fish getting eaten by a cat?",
    )

Let's add entities to our zero-dimensional classes::

    db_map.add_entity_item(entity_class_name="fish", name="Nemo", description="Lost (for now).")
    db_map.add_entity_item(
        entity_class_name="cat", name="Felix", description="The wonderful wonderful cat."
    )

Let's add a multi-dimensional entity to our multi-dimensional class. For this we need to specify the entity names
as `element_name_list`::

    db_map.add_entity_item(entity_class_name="fish__cat", element_name_list=("Nemo", "Felix"))

Let's add a parameter definition for one of our entity classes::

    db_map.add_parameter_definition_item(entity_class_name="fish", name="color")

Finally, let's specify a parameter value for one of our entities.
First, we use :func:`.to_database` to convert the value we want to give into a tuple of ``value`` and ``type``::

    value, type_ = api.to_database("mainly orange")

Now we create our parameter value::

    db_map.add_parameter_value_item(
        entity_class_name="fish",
        entity_byname=("Nemo",),
        parameter_definition_name="color",
        alternative_name="Base",
        value=value,
        type=type_
    )

Note that in the above, we refer to the entity by its *byname*.
We also set the value to belong to an *alternative* called ``Base``
which is readily available in new databases.

.. note::

  The data we've added so far is not yet in the DB, but only in an in-memory mapping within our ``db_map`` object.
  Don't worry, we will save it to the DB soon (see `Committing data`_ if you're impatient).

Retrieving data
---------------

To retrieve data, we use e.g. :meth:`~.DatabaseMapping.get_entity_class_item`,
:meth:`~.DatabaseMapping.get_entity_item`, etc.
This implicitly fetches data from the DB
into the in-memory mapping, if not already there.
For example, let's find one of the entities we inserted above::

    felix_item = db_map.get_entity_item(entity_class_name="cat", name="Felix")
    assert felix_item["description"] == "The wonderful wonderful cat."

Above, ``felix_item`` is a :class:`~.PublicItem` object, representing an item.

Let's find our multi-dimensional entity::

    nemo_felix_item = db_map.get_entity_item("entity", entity_class_name="fish__cat", element_name_list=("Nemo", "Felix"))
    assert nemo_felix_item["dimension_name_list"] == ('fish', 'cat')

Now let's retrieve our parameter value::

    nemo_color_item = db_map.get_parameter_value_item(
        entity_class_name="fish",
        entity_byname=("Nemo",),
        parameter_definition_name="color",
        alternative_name="Base"
    )

We use :func:`.from_database` to convert the value and type from the parameter value into our original value::

    nemo_color = api.from_database(nemo_color_item["value"], nemo_color_item["type"])
    assert nemo_color == "mainly orange"

To retrieve all the items of a given type, we use :meth:`~.DatabaseMapping.get_items`::

    assert [entity["entity_byname"] for entity in db_map.get_items("entity")] == [
        ("Nemo",), ("Felix",), ("Nemo", "Felix")
    ]

Now you should use the above to try and find Nemo.


Updating data
-------------

To update data, we use the :meth:`~.PublicItem.update` method of :class:`~.PublicItem`.

Let's rename our fish entity to avoid any copyright infringements::

    db_map.get_entity_item(entity_class_name="fish", name="Nemo").update(name="NotNemo")

To be safe, let's also change the color::

    new_value, new_type = api.to_database("not that orange")
    db_map.get_parameter_value_item(
        entity_class_name="fish",
        entity_byname=("NotNemo",),
        parameter_definition_name="color",
        alternative_name="Base",
    ).update(value=new_value, type=new_type)

Note how we need to use then new entity name ``NotNemo`` to retrieve the parameter value. This makes sense.

Removing data
-------------

You know what, let's just remove the entity entirely.
To do this we use the :meth:`~.PublicItem.remove` method of :class:`~.PublicItem`::

    db_map.get_entity_item(entity_class_name="fish", name="NotNemo").remove()

Note that the above call removes items in *cascade*,
meaning that items that depend on ``"NotNemo"`` will get removed as well.
We have one such item in the database, namely the ``"color"`` parameter value
which also gets dropped when the above method is called.

Restoring data
--------------

TODO

Committing data
---------------

Enough messing around. To save the contents of the in-memory mapping into the DB,
we use :meth:`~.DatabaseMapping.commit_session`::

    db_map.commit_session("Find Nemo, then lose him again")
