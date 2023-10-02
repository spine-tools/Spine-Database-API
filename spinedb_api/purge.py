######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Functions to purge DBs.

"""

from .db_mapping import DatabaseMapping
from .exception import SpineDBAPIError, SpineDBVersionError
from .filters.tools import clear_filter_configs
from .helpers import remove_credentials_from_url


def _ids_for_item_type(db_map, item_type):
    """Queries ids for given database item type.

    Args:
        db_map (DatabaseMapping): database map
        item_type (str): database item type

    Returns:
        set of int: item ids
    """
    sq_attr = db_map.cache_sqs[item_type]
    return {row.id for row in db_map.query(getattr(db_map, sq_attr))}


def purge_url(url, purge_settings, logger=None):
    """Removes all items of selected types from the database at a given URL.

    Args:
        url (str): database URL
        purge_settings (dict): mapping from item type to a boolean indicating whether to remove them or not
        logger (LoggerInterface, optional): logger

    Returns:
        bool: True if operation was successful, False otherwise
    """
    try:
        db_map = DatabaseMapping(url)
    except (SpineDBAPIError, SpineDBVersionError) as err:
        sanitized_url = clear_filter_configs(remove_credentials_from_url(url))
        if logger:
            logger.msg_warning.emit(f"Failed to purge url <b>{sanitized_url}</b>: {err}")
        return False
    success = purge(db_map, purge_settings, logger=logger)
    db_map.close()
    return success


def purge(db_map, purge_settings, logger=None):
    """Removes all items of selected types from a database.

    Args:
        db_map (DatabaseMapping): target database mapping
        purge_settings (dict): mapping from item type to a boolean indicating whether to remove them or not
        logger (LoggerInterface): logger

    Returns:
        bool: True if operation was successful, False otherwise
    """
    if purge_settings is None:
        # Bring all the pain
        purge_settings = {item_type: True for item_type in DatabaseMapping.ITEM_TYPES}
    removable_db_map_data = {item_type for item_type, checked in purge_settings.items() if checked}
    if removable_db_map_data:
        try:
            if logger:
                logger.msg.emit("Purging database...")
            for item_type in removable_db_map_data:
                db_map.purge_items(item_type)
            db_map.commit_session("Purge database")
            if logger:
                logger.msg.emit("Database purged")
        except SpineDBAPIError:
            if logger:
                logger.msg_error.emit("Failed to purge database.")
            return False
    return True
