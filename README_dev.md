# Developing Data Transition

## Testing `alembic` Migration

1. Edit `./spinedb_api/alembic.ini`, point `sqlalchemy.url` to a (copy of a) SQLite test database. ⚠️ Its data will be altered by the migration script.
1. Edit `./spinedb_api/alembic/versions/a973ab537da2_reencode_parameter_values.py` and temporarily change
   ```python
   new_value = transition_data(old_value)
   ```
   to
   ```python
   new_value = b'prepend_me ' + old_value
   ```
1. Within the `./spinedb_api` folder, execute
   ```bash
   alembic upgrade head
   ```
1. Open your SQLite test database in a database editor and check for changed `paramater_value`s.

## Developing the Data Transition Module

1. Edit `./spinedb_api/compat/reencode_for_data_transition.py` for development.
1. In a Python REPL, call its function `transition_data(old_json_bytes)` and check for correct output of our test cases.
1. Once this works, revert the changes of `./spinedb_api/alembic/versions/a973ab537da2_reencode_parameter_values.py` and test the above `alembic` migration again.
