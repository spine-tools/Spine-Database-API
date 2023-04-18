######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Provides :class:`.DatabaseMapping`.

"""

from .db_mapping_query_mixin import DatabaseMappingQueryMixin
from .db_mapping_base import DatabaseMappingBase
from .db_mapping_add_mixin import DatabaseMappingAddMixin
from .db_mapping_check_mixin import DatabaseMappingCheckMixin
from .db_mapping_update_mixin import DatabaseMappingUpdateMixin
from .db_mapping_remove_mixin import DatabaseMappingRemoveMixin
from .db_mapping_commit_mixin import DatabaseMappingCommitMixin
from .filters.tools import apply_filter_stack, load_filters


class DatabaseMapping(
    DatabaseMappingQueryMixin,
    DatabaseMappingCheckMixin,
    DatabaseMappingAddMixin,
    DatabaseMappingUpdateMixin,
    DatabaseMappingRemoveMixin,
    DatabaseMappingCommitMixin,
    DatabaseMappingBase,
):
    """A basic read-write database mapping.

    :param str db_url: A database URL in RFC-1738 format pointing to the database to be mapped.
    :param str username: A user name. If ``None``, it gets replaced by the string ``"anon"``.
    :param bool upgrade: Whether or not the db at the given URL should be upgraded to the most recent version.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._init_type_attributes()
        if self._filter_configs is not None:
            stack = load_filters(self._filter_configs)
            apply_filter_stack(self, stack)
