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
Tools and utilities to work with filters, manipulators and database URLs.

"""
from json import dump, load
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from .alternative_filter import (
    ALTERNATIVE_FILTER_SHORTHAND_TAG,
    ALTERNATIVE_FILTER_TYPE,
    alternative_filter_config,
    alternative_filter_from_dict,
    alternative_filter_config_to_shorthand,
    alternative_filter_shorthand_to_config,
    alternative_names_from_dict,
)
from .renamer import (
    ENTITY_CLASS_RENAMER_SHORTHAND_TAG,
    ENTITY_CLASS_RENAMER_TYPE,
    PARAMETER_RENAMER_SHORTHAND_TAG,
    PARAMETER_RENAMER_TYPE,
    entity_class_renamer_config_to_shorthand,
    entity_class_renamer_from_dict,
    entity_class_renamer_shorthand_to_config,
    parameter_renamer_config_to_shorthand,
    parameter_renamer_from_dict,
    parameter_renamer_shorthand_to_config,
)
from .scenario_filter import (
    SCENARIO_FILTER_TYPE,
    SCENARIO_SHORTHAND_TAG,
    scenario_filter_config,
    scenario_filter_config_to_shorthand,
    scenario_filter_from_dict,
    scenario_filter_shorthand_to_config,
    scenario_name_from_dict,
)
from .value_transformer import (
    VALUE_TRANSFORMER_SHORTHAND_TAG,
    VALUE_TRANSFORMER_TYPE,
    value_transformer_shorthand_to_config,
    value_transformer_from_dict,
    value_transformer_config_to_shorthand,
)
from .execution_filter import (
    EXECUTION_SHORTHAND_TAG,
    EXECUTION_FILTER_TYPE,
    execution_filter_config,
    execution_filter_config_to_shorthand,
    execution_filter_from_dict,
    execution_filter_shorthand_to_config,
)

FILTER_IDENTIFIER = "spinedbfilter"
SHORTHAND_TAG = "cfg:"


def apply_filter_stack(db_map, stack):
    """
    Applies stack of filters and manipulator to given database map.

    Args:
        db_map (DatabaseMapping): a database map
        stack (list): a stack of database filters and manipulators
    """
    appliers = {
        ALTERNATIVE_FILTER_TYPE: alternative_filter_from_dict,
        ENTITY_CLASS_RENAMER_TYPE: entity_class_renamer_from_dict,
        EXECUTION_FILTER_TYPE: execution_filter_from_dict,
        PARAMETER_RENAMER_TYPE: parameter_renamer_from_dict,
        SCENARIO_FILTER_TYPE: scenario_filter_from_dict,
        VALUE_TRANSFORMER_TYPE: value_transformer_from_dict,
    }
    for filter_ in stack:
        appliers[filter_["type"]](db_map, filter_)


def load_filters(configs):
    """
    Loads filter configurations from disk as needed and constructs a filter stack.

    Args:
        configs (list): list of filter config dicts and paths to filter configuration files

    Returns:
        list of dict: filter stack
    """
    stack = list()
    for config in configs:
        if isinstance(config, str):
            with open(config) as config_file:
                stack.append(load(config_file))
        else:
            stack.append(config)
    return stack


def store_filter(config, out):
    """
    Writes filter config to an output stream.

    Args:
        config (dict): filter config to write
        out (TextIOBase): a file-like object that supports writing
    """
    dump(config, out)


def filter_config(filter_type, value):
    """
    Creates a config dict for filter of given type.

    Args:
        filter_type (str): the filter type (e.g. "scenario_filter")
        value (object): the filter value (e.g. scenario name)

    Returns:
        dict: filter configuration
    """
    return {
        SCENARIO_FILTER_TYPE: scenario_filter_config,
        ALTERNATIVE_FILTER_TYPE: alternative_filter_config,
        EXECUTION_FILTER_TYPE: execution_filter_config,
    }[filter_type](value)


def append_filter_config(url, config):
    """
    Appends a filter config to given url.

    ``config`` can either be a configuration dictionary or a path to a JSON file that contains the dictionary.

    Args:
        url (str): base URL
        config (str or dict): path to the config file or config as a ``dict``.

    Returns:
        str: the modified URL
    """
    url = urlparse(url)
    query = parse_qs(url.query)
    filters = query.setdefault(FILTER_IDENTIFIER, list())
    if isinstance(config, dict):
        config = config_to_shorthand(config)
    if config not in filters:
        filters.append(config)
    url = url._replace(query=urlencode(query, doseq=True))
    if not url.hostname:
        url = url._replace(path="//" + url.path)
    return url.geturl()


def filter_configs(url):
    """
    Returns filter configs or file paths from given URL.

    Args:
        url (str): a URL

    Returns:
        list: a list of filter configs
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    try:
        filters = query[FILTER_IDENTIFIER]
    except KeyError:
        return []
    parsed_filters = list()
    for filter_ in filters:
        if filter_.startswith(SHORTHAND_TAG):
            parsed_filters.append(_parse_shorthand(filter_[len(SHORTHAND_TAG) :]))
        else:
            parsed_filters.append(filter_)
    return parsed_filters


def pop_filter_configs(url):
    """
    Pops filter config files and dicts from URL's query part.

    Args:
        url (str): a URL

    Returns:
        tuple: a list of filter configs and the 'popped from' URL
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    try:
        filters = query.pop(FILTER_IDENTIFIER)
    except KeyError:
        return [], url
    parsed_filters = list()
    for filter_ in filters:
        if filter_.startswith(SHORTHAND_TAG):
            parsed_filters.append(_parse_shorthand(filter_[len(SHORTHAND_TAG) :]))
        else:
            parsed_filters.append(filter_)
    parsed = parsed._replace(query=urlencode(query, doseq=True))
    if not parsed.hostname:
        parsed = parsed._replace(path="//" + parsed.path)
    return parsed_filters, urlunparse(parsed)


def clear_filter_configs(url):
    """
    Removes filter configuration queries from given URL.

    Args:
        url (str): a URL

    Returns:
        str: a cleared URL
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    try:
        del query[FILTER_IDENTIFIER]
    except KeyError:
        return url
    parsed = parsed._replace(query=urlencode(query, doseq=True), path="//" + parsed.path)
    return urlunparse(parsed)


def ensure_filtering(url, fallback_alternative=None):
    """
    Appends fallback filters to given url if it does not contain corresponding filter already.

    Args:
        url (str): database URL
        fallback_alternative (str, optional): fallback alternative if URL does not contain scenario or alternative filters

    Returns:
        str: database URL
    """
    configs = filter_configs(url)
    stack = load_filters(configs)
    if fallback_alternative is not None:
        alternative_found = False
        for config in stack:
            scenario = scenario_name_from_dict(config)
            if scenario is not None:
                alternative_found = True
                break
            alternatives = alternative_names_from_dict(config)
            if alternatives:
                alternative_found = True
                break
        if not alternative_found:
            return append_filter_config(url, alternative_filter_config([fallback_alternative]))
    return url


def config_to_shorthand(config):
    """
    Converts a filter config dictionary to shorthand.

    Args:
        config (dict): filter configuration

    Returns:
        str: config shorthand
    """
    shorthands = {
        ALTERNATIVE_FILTER_TYPE: alternative_filter_config_to_shorthand,
        ENTITY_CLASS_RENAMER_TYPE: entity_class_renamer_config_to_shorthand,
        PARAMETER_RENAMER_TYPE: parameter_renamer_config_to_shorthand,
        SCENARIO_FILTER_TYPE: scenario_filter_config_to_shorthand,
        EXECUTION_FILTER_TYPE: execution_filter_config_to_shorthand,
        VALUE_TRANSFORMER_TYPE: value_transformer_config_to_shorthand,
    }
    return SHORTHAND_TAG + shorthands[config["type"]](config)


def _parse_shorthand(shorthand):
    """
    Converts shorthand filter config into configuration dictionary.

    Args:
        shorthand (str): a shorthand config string

    Returns:
        dict: filter configuration dictionary
    """
    shorthand_parsers = {
        ALTERNATIVE_FILTER_SHORTHAND_TAG: alternative_filter_shorthand_to_config,
        ENTITY_CLASS_RENAMER_SHORTHAND_TAG: entity_class_renamer_shorthand_to_config,
        PARAMETER_RENAMER_SHORTHAND_TAG: parameter_renamer_shorthand_to_config,
        SCENARIO_SHORTHAND_TAG: scenario_filter_shorthand_to_config,
        EXECUTION_SHORTHAND_TAG: execution_filter_shorthand_to_config,
        VALUE_TRANSFORMER_SHORTHAND_TAG: value_transformer_shorthand_to_config,
    }
    tag, _, _ = shorthand.partition(":")
    return shorthand_parsers[tag](shorthand)


def name_from_dict(config):
    """
    Returns scenario name from filter config.

    Args:
        config (dict): filter configuration

    Returns:
        str: name or None if ``config`` is not a valid 'name' filter configuration
    """
    func = {SCENARIO_FILTER_TYPE: scenario_name_from_dict}.get(config["type"])
    if func is None:
        return None
    return func(config)
