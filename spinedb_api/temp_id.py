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
Temp id stuff.

"""


class TempId(int):
    _next_id = {}

    def __new__(cls, item_type):
        id_ = cls._next_id.setdefault(item_type, -1)
        cls._next_id[item_type] -= 1
        return super().__new__(cls, id_)

    def __init__(self, item_type):
        super().__init__()
        self._item_type = item_type
        self._resolve_callbacks = []

    def __repr__(self):
        return f"TempId({self._item_type}, {super().__repr__()})"

    def add_resolve_callback(self, callback):
        self._resolve_callbacks.append(callback)

    def remove_resolve_callback(self, callback):
        try:
            self._resolve_callbacks.remove(callback)
        except ValueError:
            pass

    def resolve(self, new_id):
        while self._resolve_callbacks:
            self._resolve_callbacks.pop(0)(new_id)


class TempIdDict(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.key_map = {}
        self._unbind_callbacks_by_key = {}
        for key, value in kwargs.items():
            self._bind(key, value)

    def __getitem__(self, key):
        key = self.key_map.get(key, key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        key = self.key_map.get(key, key)
        return super().get(key, default)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._bind(key, value)

    def __delitem__(self, key):
        super().__delitem__(key)
        self._unbind(key)

    def setdefault(self, key, default):
        value = super().setdefault(key, default)
        self._bind(key, value)
        return value

    def update(self, other):
        super().update(other)
        for key, value in other.items():
            self._bind(key, value)

    def pop(self, key, default):
        if key in self:
            self._unbind(key)
        return super().pop(key, default)

    def _make_value_resolve_callback(self, key):
        def callback(new_id):
            self[key] = new_id

        return callback

    def _make_value_component_resolve_callback(self, key, value, i):
        """Returns a callback to call when the given key is resolved.

        Args:
            key (TempId)
        """

        def callback(new_id, i=i):
            new_value = list(value)
            new_value[i] = new_id
            new_value = tuple(new_value)
            self[key] = new_value

        return callback

    def _make_key_resolve_callback(self, key):
        def callback(new_id):
            if key in self:
                self.key_map[key] = new_id
                self[new_id] = self.pop(key, None)

        return callback

    def _make_key_component_resolve_callback(self, key, i):
        def callback(new_id, i=i):
            if key in self:
                new_key = list(key)
                new_key[i] = new_id
                new_key = tuple(new_key)
                self.key_map[key] = new_key
                self[new_key] = self.pop(key, None)

        return callback

    def _bind(self, key, value):
        if isinstance(value, TempId):
            value.add_resolve_callback(self._make_value_resolve_callback(key))
        elif isinstance(value, tuple):
            for (i, v) in enumerate(value):
                if isinstance(v, TempId):
                    v.add_resolve_callback(self._make_value_component_resolve_callback(key, value, i))
        elif isinstance(key, TempId):
            callback = self._make_key_resolve_callback(key)
            key.add_resolve_callback(callback)
            self._unbind_callbacks_by_key.setdefault(key, []).append(lambda: key.remove_resolve_callback(callback))
        elif isinstance(key, tuple):
            for i, k in enumerate(key):
                if isinstance(k, TempId):
                    callback = self._make_key_component_resolve_callback(key, i)
                    k.add_resolve_callback(callback)
                    self._unbind_callbacks_by_key.setdefault(key, []).append(
                        lambda k=k, callback=callback: k.remove_resolve_callback(callback)
                    )

    def _unbind(self, key):
        for callback in self._unbind_callbacks_by_key.pop(key, ()):
            callback()
