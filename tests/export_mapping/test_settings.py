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
Unit tests for export settings.

:author: A. Soininen (VTT)
:date:   4.1.2021
"""

import unittest
from spinedb_api.export_mapping.settings import relationship_export, set_relationship_dimensions
from spinedb_api.export_mapping.item_export_mapping import (
    Position,
    RelationshipClassMapping,
    RelationshipClassObjectClassMapping,
    RelationshipMapping,
    RelationshipObjectMapping,
)


class TestSetRelationshipDimensions(unittest.TestCase):
    def test_change_dimensions_from_zero_to_one(self):
        mapping = relationship_export(0, 1)
        self.assertEqual(mapping.count_mappings(), 2)
        set_relationship_dimensions(mapping, 1)
        self.assertEqual(mapping.count_mappings(), 4)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                RelationshipClassMapping,
                RelationshipClassObjectClassMapping,
                RelationshipMapping,
                RelationshipObjectMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, Position.hidden, 1, Position.hidden])

    def test_change_dimension_from_one_to_zero(self):
        mapping = relationship_export(0, 1, [2], [3])
        self.assertEqual(mapping.count_mappings(), 4)
        set_relationship_dimensions(mapping, 0)
        self.assertEqual(mapping.count_mappings(), 2)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(classes, [RelationshipClassMapping, RelationshipMapping])
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, 1])

    def test_increase_dimensions(self):
        mapping = relationship_export(0, 1, [2], [3])
        self.assertEqual(mapping.count_mappings(), 4)
        set_relationship_dimensions(mapping, 2)
        self.assertEqual(mapping.count_mappings(), 6)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                RelationshipClassMapping,
                RelationshipClassObjectClassMapping,
                RelationshipClassObjectClassMapping,
                RelationshipMapping,
                RelationshipObjectMapping,
                RelationshipObjectMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, 2, Position.hidden, 1, 3, Position.hidden])

    def test_decrease_dimensions(self):
        mapping = relationship_export(0, 1, [2, 3], [4, 5])
        self.assertEqual(mapping.count_mappings(), 6)
        set_relationship_dimensions(mapping, 1)
        self.assertEqual(mapping.count_mappings(), 4)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                RelationshipClassMapping,
                RelationshipClassObjectClassMapping,
                RelationshipMapping,
                RelationshipObjectMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, 2, 1, 4])


if __name__ == "__main__":
    unittest.main()
