# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Fixed

### Removed

- Tools, features and methods have been removed.

### Deprecated

### Security
