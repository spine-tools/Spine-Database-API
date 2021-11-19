######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains the SpineDBServer class.

:authors: M. Marin (KTH)
:date:   2.2.2020
"""

from urllib.parse import urlunsplit
from contextlib import contextmanager
import socketserver
import threading
import json
import atexit
from sqlalchemy.exc import DBAPIError
from spinedb_api import __version__ as spinedb_api_version
from .db_mapping import DatabaseMapping
from .import_functions import import_data
from .helpers import ReceiveAllMixing
from .exception import SpineDBAPIError
from .parameter_value import join_value_and_type
from .filters.scenario_filter import scenario_filter_config
from .filters.tool_filter import tool_filter_config
from .filters.tools import append_filter_config

_required_client_version = 1


def _process_parameter_definition_row(row):
    value, type_ = row.pop("default_value"), row.pop("default_type")
    row["default_value"] = join_value_and_type(value, type_)
    return row


def _process_parameter_value_row(row):
    value, type_ = row.pop("value"), row.pop("type")
    row["value"] = join_value_and_type(value, type_)
    return row


def _process_parameter_value_list_row(row):
    value = row.pop("value")
    row["value"] = join_value_and_type(value, None)
    return row


def _get_row_processor(sq_name):
    if sq_name == "parameter_definition_sq":
        return _process_parameter_definition_row
    if sq_name == "parameter_value_sq":
        return _process_parameter_value_row
    if sq_name == "parameter_value_list_sq":
        return _process_parameter_value_list_row
    return lambda row: row


_open_db_maps = {}


class _DBAnswer:
    def __init__(self, result=None, error=None):
        self._result = result
        self._error = error

    def to_dict(self):
        return {"result": self._result, "error": self._error}


class DBHandler:
    """
    Helper class to do key interactions with a db, while closing the db_map afterwards.
    Used by DBRequestHandler and by SpineInterface's legacy PyCall path.
    """

    def __init__(self, db_url, upgrade, *args, logger=None, **kwargs):
        self._db_url = db_url
        self._upgrade = upgrade
        self._logger = logger
        super().__init__(*args, **kwargs)

    def _make_db_map(self, create=False):
        try:
            return DatabaseMapping(self._db_url, upgrade=self._upgrade, create=create), None
        except Exception as error:  # pylint: disable=broad-except
            return None, error

    @contextmanager
    def _db_map_context(self, create=False):
        """Obtains and yields a db_map to fulfil a request.

        Yields:
            DatabaseMapping: A db_map or None if unable to obtain a db_map
            Exception: the error, or None if all good
        """
        db_map = _open_db_maps.get(self._db_url)
        if db_map is not None:
            # Persistent case
            yield db_map, None
        else:
            # Non-persistent case, we create a new db_map and close it when done
            db_map, error = self._make_db_map(create=create)
            try:
                yield db_map, error
            finally:
                if db_map is not None:
                    db_map.connection.close()

    def get_db_url(self):
        """
        Returns:
            str: The underlying db url
        """
        return self._db_url

    def get_data(self, *args):
        """
        Returns:
            _DBAnswer: where result is a dict from subquery name to a list of items from thay subquery, if successful.

        """
        result = {}
        with self._db_map_context() as (db_map, error):
            if error:
                return _DBAnswer(error=str(error))
            for sq_name in args:
                sq = getattr(db_map, sq_name, None)
                if sq is None:
                    continue
                process_row = _get_row_processor(sq_name)
                result[sq_name] = [process_row(x._asdict()) for x in db_map.query(sq)]
        return _DBAnswer(result=result)

    def import_data(self, data, comment):
        """Imports data and commit.

        Args:
            data (dict)
            comment (str)
        Returns:
            _DBAnswer: where result is a list of import errors, if successful.
        """
        with self._db_map_context(create=True) as (db_map, error):
            if error:
                return _DBAnswer(error=str(error))
            count, errors = import_data(db_map, **data)
            if count:
                try:
                    db_map.commit_session(comment)
                except DBAPIError:
                    db_map.rollback_session()
        return _DBAnswer(result=[err.msg for err in errors])

    def open_connection(self):
        """Opens a persistent connection to the url by creating and storing a db_map.
        The same db_map will be reused for all further requests until ``close_connection`` is called.

        Returns:
            _DBAnswer: where result is True if the db_map was created successfully.
        """
        db_map, error = self._make_db_map(create=True)
        if error:
            return _DBAnswer(error=str(error))
        _open_db_maps[self._db_url] = db_map
        return _DBAnswer(result=True)

    def close_connection(self):
        """Closes the connection opened by ``open_connection``.

        Returns:
            bool: True or False, depending on whether or not there was a connection open when calling this.
        """
        db_map = _open_db_maps.pop(self._db_url, None)
        if db_map is not None:
            db_map.connection.close()
            result = True
        else:
            result = False
        return _DBAnswer(result=result)

    def call_method(self, method_name, *args):
        """Calls a method from the DatabaseMapping class.

        Args:
            method_name (str): the method name
            args: positional arguments to call the method with.

        Returns:
            _DBAnswer: where result is the return value of the method
        """
        with self._db_map_context(create=True) as (db_map, error):
            if error:
                return _DBAnswer(error=str(error))
            method = getattr(db_map, method_name)
            result = method(*args)
            return _DBAnswer(result=result)

    def apply_filters(self, filters):
        for key, value in filters.items():
            if key == "scenario":
                self._add_scenario_filter(value)
            elif key == "tool":
                self._add_tool_filter(value)
        return _DBAnswer(result=True)

    def _add_scenario_filter(self, scenario):
        config = scenario_filter_config(scenario)
        self._db_url = append_filter_config(self._db_url, config)

    def _add_tool_filter(self, tool):
        config = tool_filter_config(tool)
        self._db_url = append_filter_config(self._db_url, config)


class _CustomJSONEncoder(json.JSONEncoder):
    """A JSON encoder that handles all the special types that can come in request responses."""

    def default(self, o):
        if isinstance(o, set):
            return list(o)
        if isinstance(o, SpineDBAPIError):
            return str(o)
        if isinstance(o, _DBAnswer):
            return o.to_dict()
        return super().default(o)


class DBRequestHandler(ReceiveAllMixing, DBHandler, socketserver.BaseRequestHandler):
    """
    The request handler class for our server.
    """

    def _get_response(self):
        data = self._recvall()
        request, *extras = json.loads(data)
        if request in ("get_api_version", "get_db_url"):
            # NOTE: For these two requests we want to be compatible with the legacy server,
            # because clients version 1 and higher will use them to check if the server needs to be updated.
            # That's why we don't expand the extras so far.
            return spinedb_api_version if request == "get_api_version" else self.get_db_url()
        try:
            args, kwargs, client_version = extras
        except ValueError:
            client_version = 0
        if client_version < _required_client_version:
            return _DBAnswer(error="wrong version")  # FIXME
        handler = {
            "get_data": self.get_data,
            "import_data": self.import_data,
            "call_method": self.call_method,
            "open_connection": self.open_connection,
            "close_connection": self.close_connection,
            "apply_filters": self.apply_filters,
        }.get(request)
        if handler is None:
            return _DBAnswer(error=f"invalid request '{request}'")
        return handler(*args, **kwargs)

    def handle(self):
        response = self._get_response()
        self.request.sendall(bytes(json.dumps(response, cls=_CustomJSONEncoder) + self._EOM, self._ENCODING))


class SpineDBServer(socketserver.TCPServer):
    # NOTE:
    # We can't use the socketserver.ThreadingMixIn because it processes each request in a different thread,
    # and we want to reuse db_maps sometimes.
    # (sqlite objects can only be used in the same thread where they were created)
    allow_reuse_address = True


_servers = {}
_servers_lock = threading.Lock()


def start_spine_db_server(db_url, logger=None, upgrade=False):
    """
    Args:
        db_url (str): Spine db url
        upgrade (bool): Whether to upgrade db or not

    Returns:
        str: server url (e.g. http://127.0.0.1:54321)
    """
    host = "127.0.0.1"
    with socketserver.TCPServer((host, 0), None) as s:
        port = s.server_address[1]
    server_url = urlunsplit(('http', f'{host}:{port}', '', '', ''))
    with _servers_lock:
        server = _servers[server_url] = SpineDBServer(
            (host, port),
            lambda *args, logger=logger, **kwargs: DBRequestHandler(db_url, upgrade, *args, logger=logger, **kwargs),
        )
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    return server_url


def shutdown_spine_db_server(server_url):
    with _servers_lock:
        server = _servers.pop(server_url, None)
    if server is not None:
        server.shutdown()
        server.server_close()


@contextmanager
def closing_spine_db_server(db_url, logger=None):
    server_url = start_spine_db_server(db_url, logger=logger)
    try:
        yield server_url
    finally:
        shutdown_spine_db_server(server_url)


def _shutdown_servers():
    with _servers_lock:
        for server in _servers.values():
            server.shutdown()
            server.server_close()


atexit.register(_shutdown_servers)
