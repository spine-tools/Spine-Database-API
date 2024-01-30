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
from __future__ import annotations
from enum import auto, Enum, unique
from dataclasses import dataclass

from .item_status import Status


@unique
class Resolution(Enum):
    USE_IN_MEMORY = auto()
    USE_IN_DB = auto()


@dataclass
class Conflict:
    in_memory: MappedItemBase
    in_db: MappedItemBase


@dataclass
class Resolved(Conflict):
    resolution: Resolution

    def __init__(self, conflict, resolution):
        self.in_memory = conflict.in_memory
        self.in_db = conflict.in_db
        self.resolution = resolution


def select_in_memory_item_always(conflicts):
    return [Resolved(conflict, Resolution.USE_IN_MEMORY) for conflict in conflicts]


def select_in_db_item_always(conflicts):
    return [Resolved(conflict, Resolution.USE_IN_DB) for conflict in conflicts]


@dataclass
class KeepInMemoryAction:
    in_memory: MappedItemBase
    set_uncommitted: bool

    def __init__(self, conflict):
        self.in_memory = conflict.in_memory
        self.set_uncommitted = not conflict.in_memory.equal_ignoring_ids(conflict.in_db)


@dataclass
class UpdateInMemoryAction:
    in_memory: MappedItemBase
    in_db: MappedItemBase

    def __init__(self, conflict):
        self.in_memory = conflict.in_memory
        self.in_db = conflict.in_db


@dataclass
class ResurrectAction:
    in_memory: MappedItemBase
    in_db: MappedItemBase

    def __init__(self, conflict):
        self.in_memory = conflict.in_memory
        self.in_db = conflict.in_db


def resolved_conflict_actions(conflicts):
    for conflict in conflicts:
        if conflict.resolution == Resolution.USE_IN_MEMORY:
            yield KeepInMemoryAction(conflict)
        elif conflict.resolution == Resolution.USE_IN_DB:
            yield UpdateInMemoryAction(conflict)
        else:
            raise RuntimeError(f"unknown conflict resolution")


def resurrection_conflicts_from_resolved(conflicts):
    resurrection_conflicts = []
    for conflict in conflicts:
        if conflict.resolution != Resolution.USE_IN_DB or not conflict.in_memory.removed:
            continue
        resurrection_conflicts.append(conflict)
    return resurrection_conflicts


def make_changed_in_memory_items_dirty(conflicts):
    for conflict in conflicts:
        if conflict.resolution != Resolution.USE_IN_MEMORY:
            continue
        if conflict.in_memory.removed:
            conflict.in_memory.status = Status.to_remove
        elif conflict.in_memory.asdict_() != conflict.in_db:
            conflict.in_memory.status = Status.to_update
