.. _user_guide:

**********
User guide
**********

It is recommended to go through the :ref:`tutorial` if you have not already
to get a grasp of the basic concepts of Spine Database API.

The code snippets in this chapter expect you to have imported :mod:`spinedb_api` as :literal:`api`.

Using an existing database
--------------------------

If you have a URL of an existing Spine database, you can create a database mapping by::

    with api.DatabaseMapping(url) as db_map:
        ...

The URL can be a string or an :class:`sqlalchemy.engine.url.URL` object.
Currently supported URL schemas are :literal:`sqlite` and :literal:`mysql`.
Note, that absolute file paths to SQLite files on Windows systems require an extra :literal:`/` before the path::

    url = "sqlite:///C:\\path\\to\\database.sqlite"

:class:`.DatabaseMapping` raises a :class:`~.SpineDBAPIError`
if it fails to open the database.
If the exception is :class:`~.SpineDBVersionError` then the database is not compatible with
the current version of :mod:`spinedb_api`.
In this case the database can be migrated to the latest schema version with the :literal:`upgrade` flag::

    with api.DatabaseMapping(url, upgrade=True) as db_map:
        ...

Optionally, you can back up the database before the upgrade::

    backup_url = "sqlite:///c:\\backups\\database.sqlite"
    with api.DatabaseMapping(url, upgrade=True, backup_url=backup_url):
        ...

Creating a new database
-----------------------

A new database can be created with the :literal:`create` flag::

    with api.DatabaseMapping(url, create=True) as db_map:
        ...

The flag has no effect if the URL points to an existing Spine database.
The contents of such databases will be spared.

.. warning::

    If an existing database contains tables that are not in the Spine database schema, the tables will be dropped
    when ``create=True``.

.. note::

    Freshly created Spine databases are otherwise empty except for an alternative called *Base*
    and a corresponding commit.

For a hassle-free experience, you may want to use both ``create`` and ``upgrade`` flags
which create a new database if it does not exist
and upgrade it automatically if it does::

    with api.DatabaseMapping(url, create=True, upgrade=True):
        ...

Sometimes it is useful to have a temporary in-memory database for testing purposes.
Such a database can be created with a special URL::

    with api.DatabaseMapping("sqlite://", create=True) as db_map:
        ...

The in-memory database will vanish when the ``with`` block ends.

About items
-----------

The public methods of :class:`.DatabaseMapping` often return :class:`~.PublicItem` objects.
These are dict-like objects that are backed by the :class:`.DatabaseMapping`.
They are dynamic:
all updates to the :class:`.DatabaseMapping` are instantly seen by its :class:`~.PublicItem` objects.

The values of an item can be accessed with the usual subscript operator::

    with api.DatabaseMapping(url) as db_map:
        alternative = db_map.alternative(name="Base")
        name = alternative["name"]
        description = alternative["description"]

A :class:`~.PublicItem` can be converted to regular dict by its :meth:`~.PublicItem._asdict` method.

While it is not possible to set the values of a :class:`~.PublicItem` with the subscript operator,
its :meth:`~.PublicItem.update` method will do the job::

    with api.DatabaseMapping(url) as db_map:
        alternative = db_map.alternative(name="Base")
        alternative.update(description="Contains data common to all scenarios.")

Refer to `Updating items`_ for updating items directly with :class:`.DatabaseMapping`.

The item can be removed from :class:`.DatabaseMapping` with :meth:`~.PublicItem.remove`
and restored back with :meth:`~.PublicItem.restore`.
A removed item is also *invalid*::

    with api.DatabaseMapping(url) as db_map:
        alternative = db_map.alternative(name="Base")
        alternative.remove()
        assert not alternative.is_valid()
        alternative.restore()
        assert alternative.is_valid()

Refer to `Removing items and purging`_ and `Restoring items`_ for removing and restoring items directly with :class:`.DatabaseMapping`.

.. note::

    Items of the *commit* type are special:
    they cannot be added, updated or removed.
    Commits are added to :class:`.DatabaseMapping` automatically
    when saving changes with the :meth:`.DatabaseMapping.commit_session` method.

Unique keys and ids
-------------------

There is two ways of identifying an item in :class:`.DatabaseMapping`: *unique key* and *id*.
Any item can be identified by either of those.

Unique key means the names or records that are needed to identify an item uniquely.
It comes from the uniqueness constraints of the Spine database schema.
For example, an entity class can be identified solely by its name.
However, to identify an entity, both its name and its entity class' name are required
since entity names are unique only within a class.
Unique keys are global in the sense that they can be used interchangeably between different :class:`.DatabaseMapping` instances.

Ids, on the other hand, are negative integers that identify an item directly.
Unlike unique keys, they are specific to a :class:`.DatabaseMapping` instance.
An entity id in one :class:`.DatabaseMapping` may refer to a different entity or be absent in another :class:`DatabaseMapping`.
The ids are represented by :class:`~.TempId` objects
and can be accessed by the ``"id"`` field of :class:`~.PublicItem`::

    with api.DatabaseMapping(url) as db_map:
        alternative = db_map.alternative(name="Base")
        item_id = alternative["id"]
        db_map.update_alternative(id=item_id, description="Basis of all scenarios.")

.. note::

    The ids used by :class:`.DatabaseMapping` objects are not equal to whatever ids the backing database may have.
    A :class:`DatabaseMapping` may contain items that have not yet been committed to a database but still need an id.
    If needed, the database id can be accesses using the :attr:`~.TempId.db_id` property of :class:`~.TempId`.

Which identification method should be used, then?
Unique keys are the only way when the id is not known.
A specific case is before an id has been assigned, i.e. before adding an item to :class:`.DatabaseMapping`.
Also, when accessing the same item in multiple instances of :class:`.DatabaseMapping`
a unique key must be used.
However, unique keys need a complex lookup and are therefore slower to use than ids.
If performance is a priority, ids should be preferred.
They may also result in simpler code,
e.g. the unique key for parameter values consists of
entity class name, entity byname, parameter definition name and alternative name
whereas their ids are just single entries.

.. note::

    If id is given to any :class:`.DatabaseMapping` method that accepts it,
    it will be used over any other keyword arguments given to the method.
    This allows, for example, renaming items with the update methods
    since the ``name``, which is usually the unique key, can then be used as the new name.

Finding items
-------------

The simplest way of getting a specific item out of a :class:`.DatabaseMapping` is to use one of the convenience methods
named after the item itself::

    with api.DatabaseMapping(url) as db_map:
        spoon = db_map.entity(entity_class_name="utensil", name="spoon")

A full unique key or id must be provided as keyword arguments to identify the item.
:class:`~.SpineDBAPIError` will be raised if the item is not found.

:meth:`.DatabaseMapping.find` and its convenience methods are useful
when searching for multiple items or when the full unique key is not available::

    with api.DatabaseMapping(url) as db_map:
        utensils = db_map.find_entities(entity_class_name="utensil")
        for utensil in utensils:
            print(f"{utensil['name']}: {utensil['description']}")

The find methods return lists of all items of given type when called without keyword arguments.
For example, this gives all parameter definition items::

    with api.DatabaseMapping(url) as db_map:
        all_definitions = db_map.find_parameter_definitions()

It is also possible to search using other fields than unique keys::

    with api.DatabaseMapping(url) as db_map:
        pointy_items = db_map.find_entities(description="Pointy one.")
        print("Pointy items:")
        for item in pointy_items:
            print(item["name"])

"Anything goes" values inside dimension name lists, entity bynames and other list-like fields
can be replaced with the ``Asterisk`` placeholder::

    with api.DatabaseMapping(url) as db_map:
        utensil_relationship_classes = db_map.find_entity_classes(
            dimension_name_list=[api.helpers.Asterisk, "utensil"]
        )

Bare :meth:`.DatabaseMapping.find` might be useful when more generic programming is required::

    with api.DatabaseMapping(url) as db_map:
        stuff = {}
        for item_type in ("scenario", "alternative"):
            table = db_map.mapped_table(item_type)
            stuff[item_type] = db_map.find(table)

Adding items
------------

Adding just a few item is best done using the convenience methods::

    with api.DatabaseMapping(url) as db_map:
        db_map.add_entity_class(name="utensil")
        db_map.add_entity(entity_class_name="utensil", name="spoon")

Methods that add a single item return the added item as :class:`PublicItem`.

Multiple items can be added using the pluralized convenience methods::

    with api.DatabaseMapping(url) as db_map:
        db_map.add_entity_class(name="utensil")
        db_map.add_entities(
            [
                {"entity_class_name": "utensil", "name": "spoon"},
                {"entity_class_name": "utensil", "name": "fork", "description": "Spiky one."}
            ]
        )

The common entries in the dicts above can be given as keyword arguments::

    with api.DatabaseMapping(url) as db_map:
        db_map.add_entity_class(name="utensil")
        db_map.add_entities(
            [{"name": "spoon"}, {"name": "fork", "description": "Spiky one."}],
            entity_class_name="utensil"
        )


The pluralized add methods may not be ideal e.g. when you have the items types available as strings.
In this case you can use :meth:`.DatabaseMapping.add` directly::

    with api.DatabaseMapping(url) as db_map:
        additional_items = {
            "entity_class": [{"name": "utensil"}],
            "entity": [
                {"entity_class_name": "utensil", "name": "spoon"},
                {"entity_class_name": "utensil", "name": "fork", "description": "Spiky one."}
            ],
        }
        for item_type, items in additional_items.items():
            table = db_map.mapped_table(item_type)
            for item in items:
                db_map.add(table, **item)

The methods that add a single item also return the added item as :class:`~.PublicItem`.

All methods that add items will raise :class:`~.SpineDBAPIError` if something goes wrong,
e.g. when adding a duplicate item.

.. note::

    Items can be added only when the items they depend on are already in the database mapping.
    For example, an entity class must exist before entities can be added to it.

Updating items
--------------

Besides the :meth:`~.PublicItem.update` method of :class:`~.PublicItem` discussed in `About items`_,
:class:`.DatabaseMapping` offers methods to update and modify items.

Single items can be updated with the convenience update methods::

    with api.DatabaseMapping(url) as db_map:
        db_map.update_entity_class(name="utensil", description="Tools for eating.")

In the above, ``name`` is used as a unique key to find the entity class item.
If the unique key is going to be modified, the id of the item must be used for identification::

    with api.DatabaseMapping(url) as db_map:
        entity_class = db_map.entity_class(name="utensil")
        db_map.update_entity_class(id=entity_class["id"], name="tableware")

The methods that update a single single also return that item as :class:`PublicItem`.

The pluralized update methods allow updating multiple items in one go.
Update data is supplied as list of dicts and common entries can optionally be given as keyword arguments::

    with api.DatabaseMapping(url) as db_map:
        new_weights = [
            {"entity_byname": ("fork",), "alternative_name": "Base", "parsed_value": 0.02},
            {"entity_byname": ("fork",), "alternative_name": "heavy_pointy_things", "parsed_value": 0.03},
            {"entity_byname": ("spoon",), "alternative_name": "Base", "parsed_value": 0.02},
        ]
        db_map.update_parameter_values(
            new_weights,
            entity_class_name="utensil",
            parameter_definition_name="weight",
        )

Under the hood, every update method uses :meth:`.DatabaseMapping.update`.
Sometimes it makes sense to use it directly::

    description_updates = {
        "alternative": [
            {"name": "heavy_pointy_things", "description": "Forks made of wolfram?"}
        ],
        "scenario": [
            {"name": "all_things_wolfram", "description": "When eating becomes a workout."},
        ],
    }
    with api.DatabaseMapping(url) as db_map:
        for item_type, updates in description_update.items():
            table = db_map.mapped_table(item_type)
            for update in updates:
                db_map.update(table, **update)

The update methods will raise :class:`~.SpineDBAPIError` in case of errors.

Flexible adds/updates
---------------------

Sometimes there is need to modify an item and, if it does not exists, create it.
This common operation is somewhat tedious with the update and add methods.
Therefore, :class:`.DatabaseMapping` provides :meth:`.DatabaseMapping.add_or_update`
and its convenience methods.
They work much like the add and update methods described above.

Removing items and purging
--------------------------

.. note::

    Items are removed in *cascade* meaning that all items that depend on the removed item are also removed.

If you have an instance of :class:`~.PublicItem`, you can just call its :meth:`~.PublicItem.remove` method
to remove it as discussed in `About items`_,
:class:`.DatabaseMapping` has further methods to remove items::

    with api.DatabaseMapping(url) as db_map:
        db_map.remove_entity(entity_class_name="cutlery", name="spoon")

Pluralized versions of the convenience methods are useful when removing multiple items::

    with api.DatabaseMapping(url) as db_map:
        db_map.remove_entities([{"name": "fork"}, {"name": "spoon"}], entity_class_name="cutlery")

The base :meth:`.DatabaseMapping.remove` is sometimes useful as well::

    for_removal = {
        "alternative": ["heavy_pointy_things", "dull_pointy_things"],
        "scenario": ["all_things_wolfram",],
    }
    with api.DatabaseMapping(url) as db_map:
        for item_type, names in for_removal.items():
            table = db_map.mapped_table(item_type)
            for name in names:
                db_map.remove(table, name=name)

The remove methods will raise :class:`~.SpineDBAPIError` if the item is not found.

Restoring items
---------------

While :class:`~.PublicItem` offers the :meth:`~.PublicItem.restore` method,
also :class:`.DatabaseMapping` has ways to restore removed items::

    with api.DatabaseMapping(url) as db_map:
        spoon = db_map.entity(entity_class_name="cutlery", name="spoon")
        spoon.remove()
        db_map.restore_entity(id=spoon["id"])

The restore methods return the restored item as :class:`~.PublicItem`.

Multiple items can be restored in a single call with the pluralized methods::

    removed_cutlery = ["spoon", "fork"]
    with api.DatabaseMapping(url) as db_map:
        items_to_restore = [{"name": name} for name in removed_cutlery]
        db_map.restore_entities(items_to_restore, entity_class_name="cutlery")

The base :meth:`.DatabaseMapping.restore` can be used too::

    with api.DatabaseMapping(url) as db_map:
        table = db_map.mapped_table("entity")
        db_map.restore(table, entity_class_name="cutlery", name="fork")

The restore methods will raise :class:`~.SpineDBAPIError` in case the operation failed.

Committing changes
------------------

No changes are made to the backing database unless explicitly committed with :meth:`.DatabaseMapping.commit_session`.
The method requires a commit message which should describe the changes.
Most items have a *commit_id* property that references the commit item of their last modification.
This excludes structural items such as entity classes.

:meth:`.DatabaseMapping.commit_session` returns a data structure that describes any compatibility transforms
that took place during the commit
such as replacing the legacy ``"is_active"`` flags by entity alternatives.
This structure has some specialized uses in Spine Toolbox and can usually be ignored.

:meth:`.DatabaseMapping.commit_session` raises :class:`NothingToCommit` when there are no changes to save.
Other errors raise :class:`SpineDBAPIError`.
