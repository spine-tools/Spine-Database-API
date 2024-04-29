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

"""The :class:`Query` class."""

from sqlalchemy import select, and_
from sqlalchemy.sql.functions import count
from .exception import SpineDBAPIError


class Query:
    """A clone of SQL Alchemy's :class:`~sqlalchemy.orm.query.Query`."""

    def __init__(self, bind, *entities):
        """
        Args:
            bind(Engine or Connection): An engine or connection to a DB against which the query will be executed.
            entities(Iterable): A sequence of SQL expressions.
        """
        self._bind = bind
        self._entities = entities
        self._select = select(entities)
        self._from = None

    def __str__(self):
        return str(self._select)

    @property
    def column_descriptions(self):
        return [{"name": c.name} for c in self._select.columns]

    def column_names(self):
        yield from (c.name for c in self._select.columns)

    def subquery(self, name=None):
        return self._select.alias(name)

    def add_columns(self, *columns):
        self._entities += columns
        self._select = select(self._entities)
        return self

    def filter(self, *clauses):
        for clause in clauses:
            self._select = self._select.where(clause)
        return self

    def filter_by(self, **kwargs):
        if len(self._entities) != 1:
            raise SpineDBAPIError(f"can't find a unique 'from-clause' to filter, candidates are {self._entities}")
        return self.filter(and_(getattr(self._entities[0].c, k) == v for k, v in kwargs.items()))

    def _get_from(self, right, on):
        if self._from is not None:
            return self._from
        from_candidates = (set(_get_descendant_tables(on)) - {right}) & set(self._select.get_children())
        if len(from_candidates) != 1:
            raise SpineDBAPIError(f"can't find a unique 'from-clause' to join into, candidates are {from_candidates}")
        return next(iter(from_candidates))

    def join(self, right, on, isouter=False):
        self._from = self._get_from(right, on).join(right, on, isouter=isouter)
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

    def distinct(self, *args):
        self._select = self._select.distinct(*args)
        return self

    def having(self, *args):
        self._select = self._select.having(*args)
        return self

    def _result(self):
        return self._bind.execute(self._select)

    def all(self):
        return self._result().fetchall()

    def first(self):
        return self._result().first()

    def one(self):
        result = self._result()
        first = result.fetchone()
        if first is None:
            return SpineDBAPIError("no results found for one()")
        second = result.fetchone()
        if second is not None:
            raise SpineDBAPIError("multiple results found for one()")
        return first

    def one_or_none(self):
        result = self._result()
        first = result.fetchone()
        if first is None:
            return None
        second = result.fetchone()
        if second is not None:
            raise SpineDBAPIError("multiple results found for one_or_none()")
        return first

    def scalar(self):
        return self._result().scalar()

    def count(self):
        return self._bind.execute(select([count()]).select_from(self._select)).scalar()

    def __iter__(self):
        return self._result() or iter([])


def _get_leaves(parent):
    children = parent.get_children()
    if not children:
        try:
            yield parent.table
        except AttributeError:
            pass
    for child in children:
        yield from _get_leaves(child)


def _get_descendant_tables(on):
    for x in on.get_children():
        try:
            yield x.table
        except AttributeError:
            yield from _get_descendant_tables(x)
