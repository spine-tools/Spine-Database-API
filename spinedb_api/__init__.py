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

from .db_mapping import DatabaseMapping
from .diff_db_mapping import DiffDatabaseMapping
from .exception import (
    SpineDBAPIError,
    SpineIntegrityError,
    SpineDBVersionError,
    SpineTableNotFoundError,
    RecordNotFoundError,
    ParameterValueError,
    ParameterValueFormatError,
    InvalidMapping,
)
from .helpers import (
    naming_convention,
    create_spine_metadata,
    SUPPORTED_DIALECTS,
    create_new_spine_database,
    copy_database,
    is_unlocked,
    is_head,
    is_empty,
    forward_sweep,
    Asterisk,
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
    check_parameter_value_list,
)
from .import_functions import (
    import_alternatives,
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
    import_tools,
    import_features,
    import_tool_features,
    import_tool_feature_methods,
    import_metadata,
    import_object_metadata,
    import_relationship_metadata,
    import_object_parameter_value_metadata,
    import_relationship_parameter_value_metadata,
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
    export_tools,
    export_features,
    export_tool_features,
    export_tool_feature_methods,
)
from .import_mapping.import_mapping_compat import import_mapping_from_dict
from .import_mapping.generator import get_mapped_data
from .parameter_value import (
    convert_containers_to_maps,
    convert_leaf_maps_to_specialized_containers,
    convert_map_to_dict,
    convert_map_to_table,
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
    ListValueRef,
)
from .filters.alternative_filter import apply_alternative_filter_to_parameter_value_sq
from .filters.scenario_filter import apply_scenario_filter_to_parameter_value_sq
from .filters.tool_filter import apply_tool_filter_to_entity_sq
from .filters.execution_filter import apply_execution_filter
from .filters.renamer import apply_renaming_to_parameter_definition_sq, apply_renaming_to_entity_class_sq
from .filters.tools import (
    append_filter_config,
    apply_filter_stack,
    clear_filter_configs,
    config_to_shorthand,
    load_filters,
    pop_filter_configs,
    name_from_dict,
)

name = "spinedb_api"
__version__ = "0.26.0.dev0"
