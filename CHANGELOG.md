# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.33.0]

### Changed

- **Breaking change:** The required SQLAlchemy version is now 1.4.
  - The low-level query interface of `DatabaseMapping` (`entity_sq`, `alternative_sq`,...)
    now return proper SQLAlchemy 1.4 `Result` objects.
  - Using `DatabaseMapping` as a context manager (e.g. with the `with` statement)
    now opens and closes a session.
  - With the higher-level interface (`get_item()`, `add_item()`,...),
    the session is opened automatically as needed.
  - The low-level query interface requires the session to be opened manually,
    i.e. all queries must be done inside a `with` block.
  - All locking primitives have been removed from `DatabaseMapping`.
    Clients are now responsible for preventing race conditions, deadlocks
    and other multithreading/multiprocessing issues.

### Added

- Support for Python 3.13.
- Experimental and incomplete [Pandas](pandas.pydata.org) dataframe support added
  in the form of a new module `dataframes`. See the module documentation for more.
- Experimental and incomplete [Arrow](arrow.apache.org) support added in the form of `arrow_value` module.
- It is now possible to use the `parsed_value` field when adding or updating
  parameter definition, paramater value and list value items.
  `parsed_value` replaces the `value` and `type` (`default_value` and `default_type` for parameter definitions)
  fields and accepts the value directly so manual conversion using `to_database()` is not needed anymore.
- Added a read-only field `entity_class_byname` to EntityClassItem (accessible from EntityItem as well)
  which works analogously to `entity_byname`.

## [0.32.2]

### Changed

- Alternative filter now filters entities, entity groups, metadata, alternatives and scenarios.
- Scenario filter now filters entity groups.

## [0.32.1]

### Removed

- The ``codename`` field and related stuff has been removed from ``DatabaseMapping``.

## [0.32.0]

Dropped support for Python 3.8.
Spine-Database-API now requires Python 3.9 or later, up to 3.12.

### Changed

- ``commit_session()`` now raises ``NothingToCommit`` when there is nothing to commit.
  Previously, it would raise ``SpineDBAPIException``.

## [0.31.6]

### Fixed

- Fixed a bug in scenario filter

## [0.31.5]

### Changed

- ``spine_io`` now uses ``gamsapi`` module instead of ``gdxcc``.
  GAMS version 42 or later is required for ``.gdx`` import/export functionality.

## [0.31.4]

### Fixed

- Fixed issues with removing committed items, then adding new items with the same unique key.

## [0.31.3]

### Added

- parameter_type table was added to the database. It defines valid value types for a parameter definition.
  The types are not currently enforced.
- display_mode and entity_class_display_mode tables were added to the database.
  They define available display modes and how each class should be displayed under each mode.
  This is for visualization purposes only.

### Changed

- DB server version was bumped from 7 to 8 due to the changes below.
- Scalar parameters (float, string, boolean) now have a proper type in `parameter_definition`, `parameter_value` and `list_value` tables.
  Previously, the type was left unspecified (`None`).
  Consequently, `to_database()` always returns a valid type string unless the value is `None`.
  **Breaking**: A type must now be always supplied to `from_database()` explicitly.
  A new migration script adds missing type information to existing databases.

## [0.31.2]

### Changed

- It is now possible to omit `name` for `add_entity_class_item()` if `dimension_name_list` is supplied instead.
  The name of the class is then automatically generated from the dimensions.
- It is now possible to omit `name` for `add_entity_item()` if `element_name_list` is supplied instead.
  The name of the entity is then automatically generated from the dimensions.

## [0.31.1]

### Changed

- The server default of `active_by_default` is now `True`.

### Removed

- `ScenarioActiveFlag` import and export mappings have been removed.
  While the flag column is still in the database schema, it is not used anywhere,
  nor is it accessible e.g. in Toolbox.

## [0.31.0]

This is the first release where we keep a Spine-Database-API specific changelog.

The database structure has changed quite a bit.
Large parts of the API have been rewritten or replaced by new systems.
We are still keeping many old entry points for backwards compatibility,
but those functions and methods are pending deprecation.

### Changed

- Python 3.12 is now supported.
- Objects and relationships have been replaced by *entities*.
  Zero-dimensional entities correspond to objects while multidimensional entities to relationships.

### Added

- *Entity alternatives* control the visibility of entities.
  This replaces previous tools, features and methods.
- Support for *superclasses*.
  It is now possible to set a superclass for an entity class.
  The class then inherits parameter definitions from its superclass.

### Removed

- Tools, features and methods have been removed.
