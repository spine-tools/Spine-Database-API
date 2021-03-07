######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Contains functions to group values in pivot tables with hidden columns or rows.

:author: M. Marin (KTH)
:date:   7.3.2021
"""
import numpy as np


class GroupFunction:
    NAME = NotImplemented

    def __call__(self, items):
        raise NotImplementedError

    def to_dict(self):
        return {"name": self.NAME}


class GroupSum(GroupFunction):
    NAME = "sum"

    def __call__(self, items):
        if not items:
            return np.nan
        return sum(items)


class GroupConcat(GroupFunction):
    NAME = "concatenate"

    def __call__(self, items):
        if not items:
            return ""
        return ",".join([str(x) for x in items])


class GroupOneOrNone(GroupFunction):
    NAME = "one_or_none"

    def __call__(self, items):
        if not items or len(items) != 1:
            return None
        return items[0]


class NoGroup(GroupFunction):
    NAME = ""

    def __call__(self, items):
        if items is None:
            return None
        return items[0]


def from_str(name):
    """
    Creates group function from name.

    Args:
        name (str, NoneType): group function name or None if no aggregation wanted.

    Returns:
        GroupFunction or NoneType
    """
    constructor = {klass.NAME: klass for klass in (GroupSum, GroupConcat, GroupOneOrNone)}.get(name, NoGroup)
    return constructor()
