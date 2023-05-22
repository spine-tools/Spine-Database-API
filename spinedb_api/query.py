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

"""Provides :class:`.Query`."""

from .exception import SpineDBAPIError


class Query:
    def __init__(self, db_map, select_):
        self._db_map = db_map
        self._select = select_
        self._from = None

    def subquery(self, name=None):
        return self._select.alias(name)

    def column_names(self):
        yield from (column.description for column in self._select.columns)

    def add_columns(self, *columns):
        for column in columns:
            self._select.append_column(column)
        return self

    def filter(self, *args):
        self._select = self._select.where(*args)
        return self

    def _get_from(self, right, on):
        from_candidates = (set(_get_descendant_tables(on)) - {right}) & set(self._select.get_children())
        if len(from_candidates) != 1:
            raise SpineDBAPIError(f"can't find a unique 'from-clause' to join into, candidates are {from_candidates}")
        return next(iter(from_candidates))

    def join(self, right, on, isouter=False):
        from_ = self._get_from(right, on) if self._from is None else self._from
        self._from = from_.join(right, on, isouter=isouter)
        self._select = self._select.select_from(self._from)
        return self

    def outerjoin(self, right, on):
        return self.join(right, on, isouter=True)

    def order_by(self, *args):
        self._select = self._select.order_by(*args)
        return self

    def group_by(self, *args):
        self._select = self._select.group_by(*args)
        return self

    def limit(self, *args):
        self._select = self._select.limit(*args)
        return self

    def offset(self, *args):
        self._select = self._select.offset(*args)
        return self

    def all(self):
        return list(self)

    def __iter__(self):
        return self._db_map.connection.execute(self._select)


def _get_descendant_tables(on):
    for x in on.get_children():
        try:
            yield x.table
        except AttributeError:
            yield from _get_descendant_tables(x)
