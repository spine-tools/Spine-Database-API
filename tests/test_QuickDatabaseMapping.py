######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for :class:`QuickDatabaseMapping`.

:author: A. Soininen (VTT)
:date:   18.11.2020
"""

import unittest
from spinedb_api import (
    import_object_classes,
    import_objects,
    import_relationship_classes,
    import_relationships,
    QuickDatabaseMapping,
)


class TestQuickDatabaseMapping(unittest.TestCase):
    def test_import_relationships(self):
        db_map = QuickDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("object_class",))
        import_objects(db_map, (("object_class", "object"),))
        import_relationship_classes(db_map, (("relationship_class", ("object_class",)),))
        import_relationships(db_map, (("relationship_class", ("object",)),))


if __name__ == '__main__':
    unittest.main()
