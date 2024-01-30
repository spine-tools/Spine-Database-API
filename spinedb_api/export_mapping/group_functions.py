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
"""
Contains functions to group values in pivot tables with hidden columns or rows.

"""
import numpy as np


class GroupFunction:
    NAME = NotImplemented
    DISPLAY_NAME = NotImplemented

    def __call__(self, items):
        """Performs the grouping. Reduces the given list of items into a single value.

        Args:
            items (list or None)

        Returns:
            Any
        """
        raise NotImplementedError


class GroupSum(GroupFunction):
    NAME = "sum"
    DISPLAY_NAME = "sum"

    def __call__(self, items):
        if not items:
            return np.nan
        try:
            return sum(items)
        except TypeError:
            return np.nan


class GroupMean(GroupFunction):
    NAME = "mean"
    DISPLAY_NAME = "mean"

    def __call__(self, items):
        if not items:
            return np.nan
        try:
            return np.mean(items)
        except TypeError:
            return np.nan


class GroupMin(GroupFunction):
    NAME = "min"
    DISPLAY_NAME = "min"

    def __call__(self, items):
        if not items:
            return np.nan
        try:
            return min(items)
        except TypeError:
            return np.nan


class GroupMax(GroupFunction):
    NAME = "max"
    DISPLAY_NAME = "max"

    def __call__(self, items):
        if not items:
            return np.nan
        try:
            return max(items)
        except TypeError:
            return np.nan


class GroupConcat(GroupFunction):
    NAME = "concat"
    DISPLAY_NAME = "concatenate"

    def __call__(self, items):
        if not items:
            return ""
        return ",".join([str(x) for x in items])


class GroupOneOrNone(GroupFunction):
    NAME = "one_or_none"
    DISPLAY_NAME = "one or none"

    def __call__(self, items):
        if not items or len(items) != 1:
            return None
        return items[0]


class NoGroup(GroupFunction):
    NAME = "no_group"
    DISPLAY_NAME = "do not group"

    def __call__(self, items):
        if items is None:
            return None
        # The items are always in a list, even if not grouping, because we want to use the same code
        # for grouping and not grouping. If not grouping, the list will contain exactly one element.
        return items[0]


_classes = (NoGroup, GroupSum, GroupMean, GroupMin, GroupMax, GroupConcat, GroupOneOrNone)

GROUP_FUNCTION_DISPLAY_NAMES = [klass.DISPLAY_NAME for klass in _classes]


def group_function_name_from_display(display_name):
    return {klass.DISPLAY_NAME: klass.NAME for klass in _classes}.get(display_name, NoGroup.NAME)


def group_function_display_from_name(name):
    return {klass.NAME: klass.DISPLAY_NAME for klass in _classes}.get(name, NoGroup.DISPLAY_NAME)


def from_str(name):
    """
    Creates group function from name.

    Args:
        name (str, NoneType): group function name or None if no aggregation wanted.

    Returns:
        GroupFunction or NoneType
    """
    constructor = {klass.NAME: klass for klass in _classes}.get(name, NoGroup)
    return constructor()
