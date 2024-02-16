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

import uuid


class TempId:
    def __init__(self, item_type, id_map):
        super().__init__()
        self._id = uuid.uuid4()
        self._item_type = item_type
        self._id_map = id_map
        self._db_id = None

    @property
    def db_id(self):
        return self._db_id

    def __repr__(self):
        resolved_to = f" resolved to {self._db_id}" if self._db_id is not None else ""
        return f"TempId({self._item_type}){resolved_to}"

    def __eq__(self, other):
        return isinstance(other, TempId) and other._item_type == self._item_type and other._id == self._id

    def __hash__(self):
        return hash((self._item_type, self._id))

    def __lt__(self, other):
        return self._id < other._id

    def resolve(self, db_id):
        self._db_id = db_id
        self._id_map[db_id] = self

    def unresolve(self):
        self._db_id = None


def resolve(value):
    if isinstance(value, tuple):
        return tuple(resolve(v) for v in value)
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    if isinstance(value, TempId):
        return value.db_id
    return value
