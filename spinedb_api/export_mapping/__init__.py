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
This package contains facilities to map a Spine database into tables.

"""

from .generator import rows, titles
from .settings import (
    alternative_export,
    feature_export,
    object_export,
    object_group_export,
    object_parameter_default_value_export,
    object_parameter_export,
    parameter_value_list_export,
    relationship_export,
    relationship_object_parameter_default_value_export,
    relationship_object_parameter_export,
    relationship_parameter_default_value_export,
    relationship_parameter_export,
    scenario_alternative_export,
    scenario_export,
    tool_export,
    tool_feature_export,
    tool_feature_method_export,
)
