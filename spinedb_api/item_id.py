######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
from collections import Counter


class IdFactory:
    def __init__(self):
        self._next_id = -1

    def next_id(self):
        item_id = self._next_id
        self._next_id -= 1
        return item_id


class IdMap:
    def __init__(self):
        self._item_id_by_db_id = {}
        self._db_id_by_item_id = {}

    def add_item_id(self, item_id):
        self._db_id_by_item_id[item_id] = None

    def remove_item_id(self, item_id):
        db_id = self._db_id_by_item_id.pop(item_id, None)
        if db_id is not None:
            del self._item_id_by_db_id[db_id]

    def set_db_id(self, item_id, db_id):
        self._db_id_by_item_id[item_id] = db_id
        self._item_id_by_db_id[db_id] = item_id

    def remove_db_id(self, id_):
        if id_ > 0:
            item_id = self._item_id_by_db_id.pop(id_)
        else:
            item_id = id_
            db_id = self._db_id_by_item_id[item_id]
            del self._item_id_by_db_id[db_id]
        self._db_id_by_item_id[item_id] = None

    def item_id(self, db_id):
        return self._item_id_by_db_id[db_id]

    def has_db_id(self, item_id):
        return item_id in self._db_id_by_item_id

    def db_id(self, item_id):
        return self._db_id_by_item_id[item_id]

    def db_id_iter(self):
        yield from self._db_id_by_item_id
