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


class TempId:
    _next_id = {}

    def __init__(self, id_, item_type, temp_id_lookup=None):
        super().__init__()
        self._id = id_
        self._item_type = item_type
        self._temp_id_lookup = temp_id_lookup if temp_id_lookup is not None else {}
        self._db_id = None
        self._temp_id_lookup[self._id] = self

    @staticmethod
    def new_unique(item_type, temp_id_lookup):
        id_ = TempId._next_id.get(item_type, -1)
        TempId._next_id[item_type] = id_ - 1
        return TempId(id_, item_type, temp_id_lookup)

    @property
    def private_id(self):
        return self._id

    @property
    def db_id(self):
        return self._db_id

    def __repr__(self):
        resolved_to = f" resolved to {self._db_id}" if self._db_id is not None else ""
        return f"TempId({self._item_type}){resolved_to}"

    def __eq__(self, other):
        return isinstance(other, TempId) and other._item_type == self._item_type and other._id == self._id

    def __gt__(self, other):
        return isinstance(other, TempId) and other._item_type == self._item_type and self._id > other._id

    def __lt__(self, other):
        return isinstance(other, TempId) and other._item_type == self._item_type and self._id < other._id

    def __hash__(self):
        return hash((self._item_type, self._id))

    def resolve(self, db_id):
        self.unresolve()
        self._db_id = db_id
        self._temp_id_lookup[db_id] = self

    def unresolve(self):
        if self._db_id is None:
            return
        if self._temp_id_lookup[self._db_id] is self:
            del self._temp_id_lookup[self._db_id]
        self._db_id = None


def resolve(value):
    if isinstance(value, tuple):
        return tuple(resolve(v) for v in value)
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    if isinstance(value, TempId):
        return value.db_id
    return value
