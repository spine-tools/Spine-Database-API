..  spinedb_api tutorial
    Created: 18.6.2018

.. _SQLAlchemy: http://www.sqlalchemy.org/


********
Tutorial
********

The Spine DB API allows one to create and manipulate
Spine databases in a standard way, using SQLAlchemy_ as the underlying engine.
This tutorial provides a quick introduction to the usage of the package.

To begin, make sure Spine database API is installed as described at :ref:`installation`.


Database Mapping
----------------

The main means of communication with a Spine DB is the :class:`.DatabaseMapping`,
specially designed to retrieve and modify data from the DB.
To create a :class:`.DatabaseMapping`, we just pass the URL of the DB to the class constructor::

    from spinedb_api import DatabaseMapping

    url = "mysql://spine_db"  # The URL of an existing Spine DB

    with DatabaseMapping(url) as db_map:
        # Do something with db_map
        pass

The URL should be formatted following the RFC-1738 standard, as described
`here <https://docs.sqlalchemy.org/en/13/core/engines.html?highlight=database%20urls#database-urls>`_.

.. note::

  Currently supported database backends are only SQLite and MySQL. More will be added later.

Creating a DB
-------------

If you're following this tutorial, chances are you don't have a Spine DB to play with just yet.
We can remediate this by creating a SQLite DB (which is just a file in your system), as follows::

    from spinedb_api import DatabaseMapping

    url = "sqlite:///first.sqlite"

    with DatabaseMapping(url, create=True) as db_map:
        # Do something with db_map
        pass

The above will create a file called ``first.sqlite`` in your current working directoy.
Note that we pass the keyword argument ``create=True`` to :class:`.DatabaseMapping` to explicitely say 
that we want the DB to be created at the given URL.

.. note::

  In the remainder we will skip the above step and work directly with ``db_map``. In other words,
  all the examples below assume we are inside the ``with`` block above.

Adding data
-----------

To insert data, we use :meth:`~.DatabaseMapping.add_item`.

Let's begin the party by adding a couple of entity classes::

    db_map.add_item("entity_class", name="fish", description="It swims.")
    db_map.add_item("entity_class", name="cat", description="Eats fish.")

Now let's add a multi-dimensional entity class between the two above. For this we need to specify the class names
as `dimensions`::

    db_map.add_item(
        "entity_class",
        name="fish__cat",
        dimension_name_list=("fish", "cat"),
        description="A fish getting eaten by a cat?",
    )


Let's add entities to our zero-dimensional classes::

    db_map.add_item("entity", class_name="fish", name="Nemo", description="Lost (for now).")
    db_map.add_item(
        "entity", class_name="cat", name="Felix", description="The wonderful wonderful cat."
    )

Let's add a multi-dimensional entity to our multi-dimensional class. For this we need to specify the entity names
as `elements`::

    db_map.add_item("entity", class_name="fish__cat", element_name_list=("Nemo", "Felix"))

Let's add a parameter definition for one of our entity classes::

    db_map.add_item("parameter_definition", entity_class_name="fish", name="color")

Finally, let's specify a parameter value for one of our entities::

    db_map.add_item(
        "parameter_value",
        entity_class_name="fish",
        entity_name="Nemo",
        parameter_definition_name="color",
        value="mainly orange"
    )

.. note::

  The data we've added so far is not yet in the DB, but only in a in-memory mapping within our ``db_map`` object.


Retrieving data
---------------

To retrieve data from the DB, we use :meth:`~.DatabaseMapping.get_item`.
For example, let's find one of the entities we inserted above::

    felix = db_map.get_item("entity", class_name="cat", name="Felix")
    print(felix["description"])  # Prints 'The wonderful wonderful cat.'


Above, ``felix`` is a :class:`~.PublicItem` object, representing an item (or row) in a Spine DB.

Let's find our multi-dimensional entity::

    nemo_felix = db_map.get_item("entity", class_name="fish__cat", byname=("Nemo", "Felix"))
    print(nemo_felix["dimension_name_list"])  # Prints '(fish, cat)'

To retrieve all the items of a given type, we use :meth:`~.DatabaseMapping.get_items`::

    print(entity["byname"] for entity in db_map.get_items("entity"))
    # Prints [("Nemo",), ("Felix",), ("Nemo", "Felix"),]

.. note::

  You should use the above to try and find Nemo!


Updating data
-------------

To update data, we use the :meth:`~.PublicItem.update` method of :class:`~.PublicItem`.

Let's rename our fish entity to avoid any copyright infringements::

    db_map.get_item("entity", class_name="fish", name="Nemo").update(name="NotNemo")

To be safe, let's also change the color::

    db_map.get_item(
        "parameter_value",
        entity_class_name="fish",
        parameter_definition_name="color",
        entity_name="NotNemo"
    ).update(value="definitely purple")


Note how we need to use then new entity name (``"NotNemo"``) to retrieve the parameter value. This makes sense.

Removing data
-------------

You know what, let's just remove the entity entirely.
To do this we use the :meth:`~.PublicItem.remove` method of :class:`~.PublicItem`::

    db_map.get_item("entity", class_name="fish", name="NotNemo").remove()







