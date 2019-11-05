######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

from .database_mapping import DatabaseMapping
from .diff_database_mapping import DiffDatabaseMapping
from .exception import (
    SpineDBAPIError,
    SpineIntegrityError,
    SpineDBVersionError,
    SpineTableNotFoundError,
    RecordNotFoundError,
    ParameterValueError,
    ParameterValueFormatError,
    TypeConversionError,
)
from .helpers import (
    naming_convention,
    SUPPORTED_DIALECTS,
    create_new_spine_database,
    copy_database,
    is_unlocked,
    is_head,
    is_empty,
    forward_sweep,
)
from .check_functions import (
    check_object_class,
    check_object,
    check_wide_relationship_class,
    check_wide_relationship,
    check_parameter_definition,
    check_parameter_value,
    check_parameter_tag,
    check_parameter_definition_tag,
    check_wide_parameter_value_list,
)
from .import_functions import (
    import_data,
    import_object_classes,
    import_objects,
    import_object_parameters,
    import_object_parameter_values,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
)
from .json_mapping import (
    mappingbase_from_dict_int_str,
    ColumnMapping,
    parameter_mapping_from_dict,
    ParameterDefinitionMapping,
    ObjectClassMapping,
    RelationshipClassMapping,
    read_with_mapping,
    dict_to_map,
    mapping_non_pivoted_columns,
)
from .parameter_value import (
    duration_to_relativedelta,
    relativedelta_to_duration,
    from_database,
    to_database,
    DateTime,
    Duration,
    IndexedValue,
    TimePattern,
    TimeSeries,
    TimeSeriesFixedResolution,
    TimeSeriesVariableResolution,
)
from .version import __version__

name = "spinedb_api"
