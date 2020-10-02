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
Tools and utilities to embed filtering information to database URLs.

:author: A. Soininen
:date:   7.10.2020
"""
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


def append_filter_config(url, config_path):
    """
    Appends a filter config file to given url.

    Args:
        url (str): base URL
        config_path (str): path to the config file

    Returns:
        str: the modified URL
    """
    url = urlparse(url)
    query = parse_qs(url.query)
    filters = query.setdefault("spinedbfilter", list())
    filters.append(config_path)
    url = url._replace(query=urlencode(query, doseq=True), path="//" + url.path)
    return url.geturl()


def pop_filter_configs(url):
    """
    Returns paths to filter config files and removes them from the URL's query part.

    Args:
        url (str): a URL

    Returns:
        tuple: a list of filter config file paths and the 'popped from' URL
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    try:
        filters = query.pop("spinedbfilter")
    except KeyError:
        return [], url
    parsed = parsed._replace(query=urlencode(query, doseq=True), path="//" + parsed.path)
    return filters, urlunparse(parsed)


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
        del query["spinedbfilter"]
    except KeyError:
        return url
    parsed = parsed._replace(query=urlencode(query, doseq=True), path="//" + parsed.path)
    return urlunparse(parsed)
