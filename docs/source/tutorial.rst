..  spinedb_api tutorial
    Created: 18.6.2018

.. _SQLAlchemy: http://www.sqlalchemy.org/

.. _tutorial:

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

    url = "mysql+pymysql://spine_db"  # The URL of an existing Spine DB

    with DatabaseMapping(url) as db_map:
        # Do something with db_map
        pass

The URL should be formatted following the RFC-1738 standard, as described
`here <https://docs.sqlalchemy.org/en/14/core/engines.html?highlight=database%20urls#database-urls>`_.

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
that we want the DB to be created at the given URL
if it does not exists already.

.. note::

  In the remainder we will skip the above step and work directly with ``db_map``. In other words,
  all the examples below assume we are inside the ``with`` block above.

Adding data
-----------

To insert data, we use e.g. :meth:`~.DatabaseMapping.add_entity_class`, :meth:`~.DatabaseMapping.add_entity`,
and so on.

Let's begin the party by adding an entity class::

    db_map.add_entity_class(name="fish", description="It swims.")

The ``add_*`` methods return the added items as instances of :class:`~.PublicItem` that behave like dicts.
Let's add another entity class and check some of its properties::

    cat_class = db_map.add_entity_class(name="cat", description="Eats fish.")
    print(f"{cat_class['name']} is known for: {cat_class['description']}")

The insertion methods will raise :class:`~.SpineDBAPIError` if database integrity would be lost
with the insertion, e.g. if you try to add a duplicate fish class::

    try:
        db_map.add_entity_class(name="fish")
    except api.SpineDBAPIError:
        print("We almost had too many classes of fish.")

Now let's add a multi-dimensional entity class to create a relationship between cat and fish.
For this we need to specify the class names as ``dimension_name_list``::

    relationship_class = db_map.add_entity_class(
        dimension_name_list=("fish", "cat"),
        description="A fish getting eaten by a cat?",
    )
    print(f"The generated class name was: {relationship_class['name']}")

In the above, we omitted the ``name`` parameter in which case the multi-dimensional class
gets named after its dimensions automatically.
If you do not fancy auto-generated names, just assign the desired value to the ``name`` parameter.

Let's add entities to our zero-dimensional classes::

    db_map.add_entity(entity_class_name="fish", name="Nemo", description="Lost (for now).")
    db_map.add_entity(
        entity_class_name="cat", name="Felix", description="The wonderful wonderful cat."
    )

Let's add a multi-dimensional entity to our multi-dimensional class. For this we need to specify the entity names
as ``element_name_list``::

    relationship = db_map.add_entity(entity_class_name="fish__cat", element_name_list=("Nemo", "Felix"))
    print(f"The generated entity name was {relationship['name']}")

Similarly to entity classes, multi-dimensional entities are named after their elements in ``element_name_list``
unless ``name`` is given explicitly.

Let's add a parameter definition for one of our entity classes::

    db_map.add_parameter_definition(entity_class_name="fish", name="color")

Finally, let's specify a parameter value for one of our entities::

    db_map.add_parameter_value(
        entity_class_name="fish",
        entity_byname=("Nemo",),
        parameter_definition_name="color",
        alternative_name="Base",
        parsed_value="mainly orange"
    )

Note that in the above, we refer to the entity by its *byname*.
Byname is a single-element tuple containing the name of the entity if it is 0-dimensional like ``("Nemo",)`` above.
For multi-dimensional entities, byname consists of the entity's 0-dimensional elements
such as ``("Nemo", "Felix")`` for the ``"fish__cat"`` entity.
At a quick glance, ``entity_byname`` and ``element_name_list`` may look the same
but this is true only for certain cases.
``element_name_list`` contains the names of the n-1 dimensional elements of n-dimensional entities
while ``entity_byname`` breaks the element names down to their 0-dimensional parts, or contains just the entity's name.

We also set the value to belong to an *alternative* called ``"Base"``
which is readily available in new databases.

.. note::

  The data we've added so far is not yet in the DB, but only in an in-memory mapping within our ``db_map`` object.
  Don't worry, we will save it to the DB soon (see `Committing data`_ if you're impatient).

Finding and retrieving data
---------------------------

To retrieve existing items, we use e.g. :meth:`~.DatabaseMapping.entity_class`,
:meth:`~.DatabaseMapping.entity`, etc.
This implicitly fetches data from the DB
into the in-memory mapping, if not already there.
For example, let's find one of the entities we inserted above::

    felix_item = db_map.entity(entity_class_name="cat", name="Felix")
    assert felix_item["description"] == "The wonderful wonderful cat."

Above, ``felix_item`` is an instance of :class:`~.PublicItem`, a dict-like object representing an item.

Let's find our multi-dimensional entity::

    nemo_felix_item = db_map.entity(entity_class_name="fish__cat", element_name_list=("Nemo", "Felix"))
    assert nemo_felix_item["dimension_name_list"] == ('fish', 'cat')

Now let's retrieve our parameter value::

    nemo_color_item = db_map.parameter_value(
        entity_class_name="fish",
        entity_byname=("Nemo",),
        parameter_definition_name="color",
        alternative_name="Base"
    )

We can use the ``"parsed_value"`` field to access Nemo's color::

    nemo_color = nemo_color_item["parsed_value"]
    assert nemo_color == "mainly orange"

The above methods return a single item and raise a :class:`~.SpineDBAPIError` if the item is not found::

    try:
        item = db_map.scenario(name="cats swim, fishes walk")
    except api.SpineDBAPIError as error:
        print(f"Failed to retrieve scenario: {error}")

To find multiple items of a given type, we use :meth:`~.DatabaseMapping.find_entities` etc.::

    print("Begin list of all entities:")
    for entity in db_map.find_entities():
        print(entity["name"])
    print("End list.")

The ``find_*`` methods returns all items when called without arguments.
You can narrow the results by giving keyword arguments.
Let's find all parameters we have in the fish class::

    for definition in db_map.find_parameter_definitions(entity_class_name="fish"):
        for entity in db_map.find_entities(entity_class_name="fish"):
            value_item = db_map.parameter_value(
                entity_class_name="fish",
                parameter_definition_name=definition["name"],
                entity_byname=entity["entity_byname"],
                alternative_name="Base",
            )
            print(f"{definition['name']} of {entity['name']} is {value_item['parsed_value']}")

When no items are found, the ``find_*`` methods return an empty list.

Now you should use the above to try and find Nemo.

.. note::

  Retrieving a large number of items one-by-one using the single item retrieval functions e.g. in a loop
  might be slow since each call may cause a database query.
  Before such operations, it might be wise to prefetch the data.
  For example, before getting a bunch of entity items, you could call
  ``db_map.fetch_all("entity")``.

Updating data
-------------

To update data, we use the :meth:`~.PublicItem.update` method of :class:`~.PublicItem`.

Let's rename our fish entity to avoid any copyright infringements::

    db_map.entity(entity_class_name="fish", name="Nemo").update(name="NotNemo")

To be safe, let's also change the color::

    value_item = db_map.parameter_value(
        entity_class_name="fish",
        entity_byname=("NotNemo",),
        parameter_definition_name="color",
        alternative_name="Base",
    )
    value_item.update(parsed_value="not that orange")
    print(f"{value_item['parameter_definition_name']} of {value_item['entity_byname']} is now {value_item['parsed_value']}")

Note how we need to use the new entity name ``NotNemo`` to retrieve the parameter value
since we just renamed it.

The above allows modifying any property of an item as long as it makes sense.
Let's try to turn ``"Felix"`` into something entirely different::

    try:
        db_map.entity(entity_class_name="cat", name="Felix").update(entity_class_name="ferret")
    except api.SpineDBAPIError as error:
        print(f"Failed to turn Felix into ferret: {error}")

An ``update`` method also exists in :class:`.DatabaseMapping`.
Since our fishes are not encumbered by intellectual property rights anymore,
let's update the entity class description::

    db_map.update_entity_class(name="fish", description="It swims free of copyrights.")

Note, that if we wanted to update the *name* or any other property that is needed to identify an item this way,
we must provide its id so :class:`.DatabaseMapping` can find the right item to update.
Our cat class contains only wonderful cats so let's update its name to reflect the fact::

    cat_class = db_map.entity_class(name="cat")
    db_map.update_entity_class(id=cat_class["id"], name="wonderful_cat")
    print(f"The new class name is {cat_class['name']}")

Updating an item directly using the :class:`.DatabaseMapping` instance also updates existing :class:`~.PublicItem` objects.

Removing data
-------------

You know what, let's just remove the entity entirely.
To do this we can use the :meth:`~.PublicItem.remove` method of :class:`~.PublicItem`::

    not_nemo = db_map.entity(entity_class_name="fish", name="NotNemo")
    not_nemo.remove()

Note that the above call removes items in *cascade*,
meaning that items that depend on ``"NotNemo"`` will get removed as well.
We have one such item in the database, namely the ``"color"`` parameter value
which also gets dropped when the above method is called.

Another way to remove items is to use the ``remove_*`` methods of :class:`.DatabaseMapping`.
Let's try to remove the ``"color"`` value again.
This time will raise a :class:`.SpineDBAPIError` because the item has been removed already::

    try:
        db_map.remove_parameter_value(
            entity_class_name="fish",
            parameter_definition_name="color",
            entity_byname=not_nemo["entity_byname"],
            alternative_name="Base",
        )
    except SpineDBAPIError as error:
        print(f"Failed to remove value a second time: {error}")

Perhaps surprisingly, we could still use the ``not_nemo`` item above to provide the ``entity_byname`` argument.
As we will soon see, it is sometimes useful to keep the dead around.

Restoring data
--------------

Already regretting we lost Ne... I mean the fish that is not Nemo?
It is possible to resurrect a removed item and bring back all its dependants with :meth:`~.PublicItem.restore`::

    not_nemo.restore()

The above will also bring back the ``"color"`` parameter value.

Luckily, we stored ``"NotNemo"`` in a variable ``not_nemo`` before removing it
so it was possible to call :meth:`~.PublicItem.restore` on it.

Committing data
---------------

Enough messing around. To save the contents of the in-memory mapping into the DB,
we use :meth:`~.DatabaseMapping.commit_session`::

    db_map.commit_session("Find Nemo, then lose him again")
