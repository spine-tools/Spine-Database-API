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
from .export_functions import export_data
from .helpers import ReceiveAllMixing
from .exception import SpineDBAPIError
from .filters.scenario_filter import scenario_filter_config
from .filters.tool_filter import tool_filter_config
from .filters.tools import append_filter_config, clear_filter_configs, apply_filter_stack
from .spine_db_client import SpineDBClient

_required_client_version = 2
_open_db_maps = {}

_START_OF_TAIL = '\u001f'  # Unit separator
_START_OF_ADDRESS = '\u0091'  # Private Use 1
_ADDRESS_SEP = ':'


def _make_value(v, value_type=None):
    return (v, value_type)


class _TailJSONEncoder(json.JSONEncoder):
    """
    A custom JSON encoder that accummulates bytes objects into a tail.
    The bytes object are encoded as a string pointing to the address in the tail.
    """

    def __init__(self):
        super().__init__()
        self._tail_parts = []
        self._tip = 0

    def default(self, o):
        if isinstance(o, bytes):
            self._tail_parts.append(o)
            new_tip = self._tip + len(o)
            fr, to = self._tip, new_tip - 1
            address = f"{_START_OF_ADDRESS}{fr}{_ADDRESS_SEP}{to}"
            self._tip = new_tip
            return address
        if isinstance(o, set):
            return list(o)
        if isinstance(o, SpineDBAPIError):
            return str(o)
        return super().default(o)

    @property
    def tail(self):
        return b"".join(self._tail_parts)


def _encode(o):
    """
    Encodes given object (representing a server response) into a message with the following structure:

        body | start of tail character | tail

    The body is obtained by JSON-encoding the argument, while replacing all `bytes` objects by addresses in the tail.
    The tail is computed by concatenating all `bytes` objects in the argument.
    See class:`_TailJSONEncoder`.

    Args:
        o (any): A Python object representing a server response.

    Returns:
        bytes: A message to the client.
    """
    encoder = _TailJSONEncoder()
    s = encoder.encode(o)
    return s.encode() + _START_OF_TAIL.encode() + encoder.tail


def _decode(b):
    """
    Decodes given message (representing a client request) into a Python object.
    The message must have the following structure:

        body | start of tail character | tail

    The result is obtained by JSON-decoding the body, and then replacing all the addresses with the referred `bytes`
    from the tail.

    Args:
        b (bytes): A message from the client.

    Returns:
        any: A Python object representing a client request.
    """
    body, tail = b.split(_START_OF_TAIL.encode())
    o = json.loads(body)
    return _expand_addresses_in_place(o, tail)


def _expand_addresses_in_place(o, tail):
    if isinstance(o, dict):
        for k, v in o.items():
            o[k] = _expand_addresses_in_place(v, tail)
        return o
    if isinstance(o, list):
        for k, e in enumerate(o):
            o[k] = _expand_addresses_in_place(e, tail)
        return o
    if isinstance(o, str):
        if not o.startswith(_START_OF_ADDRESS):
            return o
        address = o.lstrip(_START_OF_ADDRESS)
        fr, to = (int(x) for x in address.split(_ADDRESS_SEP))
        return tail[fr : to + 1]
    return o


class HandleDBMixin:
    def _make_db_map(self, create=True):
        try:
            return DatabaseMapping(self._db_url, upgrade=self._upgrade, create=create, memory=self._memory), None
        except Exception as error:  # pylint: disable=broad-except
            return None, error

    @contextmanager
    def _db_map_context(self, create=True):
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

    def query(self, *args):
        """
        Runs queries.

        Returns:
            dict: where result is a dict from subquery name to a list of items from thay subquery, if successful.
        """
        result = {}
        with self._db_map_context(create=False) as (db_map, error):
            if error:
                return dict(error=str(error))
            for sq_name in args:
                sq = getattr(db_map, sq_name, None)
                if sq is None:
                    continue
                result[sq_name] = [x._asdict() for x in db_map.query(sq)]
        return dict(result=result)

    def import_data(self, data, comment):
        """Imports data and commit.

        Args:
            data (dict)
            comment (str)
        Returns:
            dict: where result is a list of import errors, if successful.
        """
        with self._db_map_context() as (db_map, error):
            if error:
                return dict(error=str(error))
            count, errors = import_data(db_map, **data)
            if count and comment:
                try:
                    db_map.commit_session(comment)
                except DBAPIError:
                    db_map.rollback_session()
        return dict(result=[err.msg for err in errors])

    def export_data(self, **kwargs):
        """Exports data.

        Args:
            data (dict)
        Returns:
            dict: where result is the data exported from the db
        """
        with self._db_map_context() as (db_map, error):
            if error:
                return dict(error=str(error))
            return dict(result=export_data(db_map, make_value=_make_value, **kwargs))

    def open_connection(self):
        """Opens a persistent connection to the url by creating and storing a db_map.
        The same db_map will be reused for all further requests until ``close_connection`` is called.

        Returns:
            dict: where result is True if the db_map was created successfully.
        """
        if self._db_url in _open_db_maps:
            return dict(result=True)
        db_map, error = self._make_db_map()
        if error:
            return dict(error=str(error))
        _open_db_maps[self._db_url] = db_map
        return dict(result=True)

    def close_connection(self):
        """Closes the connection opened by ``open_connection``.

        Returns:
            dict: where result is always True
        """
        db_map = _open_db_maps.pop(self._db_url, None)
        if db_map is not None:
            db_map.connection.close()
        return dict(result=True)

    def call_method(self, method_name, *args, **kwargs):
        """Calls a method from the DatabaseMapping class.

        Args:
            method_name (str): the method name
            args: positional arguments passed to the method call
            kwargs: keyword arguments passed to the method call

        Returns:
            dict: where result is the return value of the method
        """
        with self._db_map_context() as (db_map, error):
            if error:
                return dict(error=str(error))
            method = getattr(db_map, method_name)
            result = method(*args, **kwargs)
            return dict(result=result)

    def apply_filters(self, filters):
        for key, value in filters.items():
            if key == "scenario":
                self._add_scenario_filter(value)
            elif key == "tool":
                self._add_tool_filter(value)
        return dict(result=True)

    def _add_scenario_filter(self, scenario):
        config = scenario_filter_config(scenario)
        new_db_url = append_filter_config(self._db_url, config)
        self._update_db_url(new_db_url, config)

    def _add_tool_filter(self, tool):
        config = tool_filter_config(tool)
        new_db_url = append_filter_config(self._db_url, config)
        self._update_db_url(new_db_url, config)

    def clear_filters(self):
        new_db_url = clear_filter_configs(self._db_url)
        self._update_db_url(new_db_url)
        return dict(result=True)

    def _update_db_url(self, new_db_url, config=None):
        db_map = _open_db_maps.pop(self._db_url, None)
        if db_map is not None:
            _open_db_maps[new_db_url] = db_map
            if config is not None:
                apply_filter_stack(db_map, [config])
            else:
                # clear filters
                db_map.restore_entity_sq_maker()
                db_map.restore_entity_class_sq_maker()
                db_map.restore_parameter_definition_sq_maker()
                db_map.restore_parameter_value_sq_maker()
        self._db_url = new_db_url

    def _get_response(self, request):
        request, *extras = _decode(request)
        # NOTE: Clients should always send requests "get_api_version" and "get_db_url" in a format that is compatible
        # with the legacy server -- to (based on the format of the answer) determine that it needs to be updated.
        # That's why we don't expand the extras so far.
        response = {"get_api_version": spinedb_api_version, "get_db_url": self.get_db_url()}.get(request)
        if response is not None:
            return response
        try:
            args, kwargs, client_version = extras
        except ValueError:
            client_version = 0
        if client_version < _required_client_version:
            return dict(error=1, result=_required_client_version)
        handler = {
            "query": self.query,
            "import_data": self.import_data,
            "export_data": self.export_data,
            "call_method": self.call_method,
            "open_connection": self.open_connection,
            "close_connection": self.close_connection,
            "apply_filters": self.apply_filters,
            "clear_filters": self.clear_filters,
        }.get(request)
        if handler is None:
            return dict(error=f"invalid request '{request}'")
        return handler(*args, **kwargs)

    def handle_request(self, request):
        response = self._get_response(request)
        return _encode(response)


class DBHandler(HandleDBMixin):
    def __init__(self, db_url, upgrade):
        self._db_url = db_url
        self._upgrade = upgrade
        self._memory = False


class DBRequestHandler(ReceiveAllMixing, HandleDBMixin, socketserver.BaseRequestHandler):
    """
    The request handler class for our server.
    """

    @property
    def _db_url(self):
        return self.server.db_url

    @_db_url.setter
    def _db_url(self, db_url):
        self.server.db_url = db_url

    @property
    def _upgrade(self):
        return self.server.upgrade

    @property
    def _memory(self):
        return self.server.memory

    def handle(self):
        request = self._recvall()
        response = self.handle_request(request)
        self.request.sendall(response + bytes(self._EOT, self._ENCODING))


class SpineDBServer(socketserver.TCPServer):
    # NOTE:
    # We can't use the socketserver.ThreadingMixIn because it processes each request in a different thread,
    # and we want to reuse db_maps sometimes.
    # (sqlite objects can only be used in the same thread where they were created)
    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass, db_url, upgrade, memory):
        super().__init__(server_address, RequestHandlerClass)
        self.db_url = db_url
        self.upgrade = upgrade
        self.memory = memory


_servers = {}
_servers_lock = threading.Lock()


def start_spine_db_server(db_url, upgrade=False, memory=False):
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
        server = _servers[server_url] = SpineDBServer((host, port), DBRequestHandler, db_url, upgrade, memory)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    return server_url


def shutdown_spine_db_server(server_url):
    with _servers_lock:
        server = _servers.pop(server_url, None)
    if server is not None:
        _teardown_server(server_url, server)


def _teardown_server(server_url, server):
    SpineDBClient.from_server_url(server_url).close_connection()
    server.shutdown()
    server.server_close()


@contextmanager
def closing_spine_db_server(db_url, upgrade=False, memory=False):
    server_url = start_spine_db_server(db_url, memory=memory)
    if memory:
        client = SpineDBClient.from_server_url(server_url)
        client.open_connection()
    try:
        yield server_url
    finally:
        if memory:
            client.close_connection()
        shutdown_spine_db_server(server_url)


def _shutdown_servers():
    with _servers_lock:
        for server_url, server in _servers.items():
            _teardown_server(server_url, server)


atexit.register(_shutdown_servers)
