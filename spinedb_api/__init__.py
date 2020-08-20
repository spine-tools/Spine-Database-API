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

from .db_mapping import DatabaseMapping
from .diff_db_mapping import DiffDatabaseMapping
from .scenario_filter_mapping import ScenarioFilterMapping
from .exception import (
    SpineDBAPIError,
    SpineIntegrityError,
    SpineDBVersionError,
    SpineTableNotFoundError,
    RecordNotFoundError,
    ParameterValueError,
    ParameterValueFormatError,
    TypeConversionError,
    InvalidMapping,
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
    Anyone,
)
from .check_functions import (
    check_alternative,
    check_scenario,
    check_scenario_alternative,
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
    import_alternatives,
    import_data_to_url,
    import_data,
    import_object_classes,
    import_objects,
    import_object_parameters,
    import_object_parameter_values,
    import_parameter_value_lists,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    import_scenarios,
    import_scenario_alternatives,
    get_data_for_import,
)
from .export_functions import (
    export_alternatives,
    export_data,
    export_object_classes,
    export_object_groups,
    export_object_parameters,
    export_object_parameter_values,
    export_objects,
    export_relationship_classes,
    export_relationship_parameter_values,
    export_relationship_parameters,
    export_relationships,
    export_scenario_alternatives,
    export_scenarios,
)
from .json_mapping import (
    AlternativeMapping,
    ColumnHeaderMapping,
    ColumnMapping,
    ConstantMapping,
    dict_to_map,
    EntityClassMapping,
    mapping_from_dict,
    mapping_non_pivoted_columns,
    mappingbase_from_dict_int_str,
    MappingBase,
    NoneMapping,
    ObjectClassMapping,
    ObjectGroupMapping,
    parameter_mapping_from_dict,
    ParameterArrayMapping,
    ParameterDefinitionMapping,
    ParameterMapMapping,
    ParameterTimePatternMapping,
    ParameterTimeSeriesMapping,
    ParameterValueMapping,
    read_with_mapping,
    RelationshipClassMapping,
    RowMapping,
    ScenarioAlternativeMapping,
    ScenarioMapping,
    TableNameMapping,
    TimeSeriesOptions,
)
from .parameter_value import (
    convert_leaf_maps_to_specialized_containers,
    duration_to_relativedelta,
    relativedelta_to_duration,
    from_database,
    to_database,
    Array,
    DateTime,
    Duration,
    IndexedNumberArray,
    IndexedValue,
    Map,
    TimePattern,
    TimeSeries,
    TimeSeriesFixedResolution,
    TimeSeriesVariableResolution,
    ParameterValueEncoder,
)
from .version import __version__

name = "spinedb_api"
