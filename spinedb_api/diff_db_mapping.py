######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Provides :class:`DiffDatabaseMapping`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from .db_mapping_query_mixin import DatabaseMappingQueryMixin
from .db_mapping_check_mixin import DatabaseMappingCheckMixin
from .diff_db_mapping_add_mixin import DiffDatabaseMappingAddMixin
from .diff_db_mapping_update_mixin import DiffDatabaseMappingUpdateMixin
from .diff_db_mapping_remove_mixin import DiffDatabaseMappingRemoveMixin
from .diff_db_mapping_commit_mixin import DiffDatabaseMappingCommitMixin
from .diff_db_mapping_base import DiffDatabaseMappingBase
from .filters.filter_stacks import apply_filter_stack, load_filters


class DiffDatabaseMapping(
    DatabaseMappingQueryMixin,
    DatabaseMappingCheckMixin,
    DiffDatabaseMappingAddMixin,
    DiffDatabaseMappingUpdateMixin,
    DiffDatabaseMappingRemoveMixin,
    DiffDatabaseMappingCommitMixin,
    DiffDatabaseMappingBase,
):
    """A read-write database mapping.

    Provides methods to *stage* any number of changes (namely, ``INSERT``, ``UPDATE`` and ``REMOVE`` operations)
    over a Spine database, as well as to commit or rollback the batch of changes.

    For convenience, querying this mapping return results *as if* all the staged changes were already committed.

    :param str db_url: A database URL in RFC-1738 format pointing to the database to be mapped.
    :param str username: A user name. If ``None``, it gets replaced by the string ``"anon"``.
    :param bool upgrade: Whether or not the db at the given URL should be upgraded to the most recent version.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self._filter_configs is not None:
            stack = load_filters(self._filter_configs)
            apply_filter_stack(self, stack)

