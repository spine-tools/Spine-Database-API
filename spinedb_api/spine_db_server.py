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
from .db_mapping import DatabaseMapping
from .import_functions import import_data
from .helpers import ReceiveAllMixing


def _add_type_information(db_value, db_type):
    """Adds type information to database value.

    Args:
        db_value (bytes): database value
        db_type (str, optional): value's type

    Returns:
        str: parameter value as JSON with an additional `type` field.
    """
    try:
        value = json.loads(db_value)
    except (TypeError, json.JSONDecodeError):
        value = None
    value = {"type": db_type, **value} if isinstance(value, dict) else value
    return json.dumps(value)


def _process_parameter_definition_row(row):
    value, type_ = row.pop("default_value"), row.pop("default_type")
    row["default_value"] = _add_type_information(value, type_)
    return row


def _process_parameter_value_row(row):
    value, type_ = row.pop("value"), row.pop("type")
    row["value"] = _add_type_information(value, type_)
    return row


def _process_parameter_value_list_row(row):
    value = row.pop("value")
    row["value"] = _add_type_information(value, None)
    return row


def _get_row_processor(sq_name):
    if sq_name == "parameter_definition_sq":
        return _process_parameter_definition_row
    if sq_name == "parameter_value_sq":
        return _process_parameter_value_row
    if sq_name == "parameter_value_list_sq":
        return _process_parameter_value_list_row
    return lambda row: row


class DBHandler:
    """
    Helper class to do key interactions with a db, while closing the db_map afterwards.
    Used by DBRequestHandler and by SpineInterface's legacy PyCall path.
    """

    def __init__(self, db_url, upgrade, *args, **kwargs):
        self._db_url = db_url
        self._upgrade = upgrade
        super().__init__(*args, **kwargs)

    def _make_db_map(self, create=False):
        try:
            return DatabaseMapping(self._db_url, upgrade=self._upgrade, create=create), None
        except Exception as error:  # pylint: disable=broad-except
            return None, error

    @contextmanager
    def _closing_db_map(self, create=False):
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
            dict: Dictionary mapping subquery names to a list of items from thay subquery, if successful.
                Dictionary {"error": "msg"}, otherwise.

        """
        data = {}
        with self._closing_db_map() as (db_map, error):
            if error:
                return {"error": str(error)}
            for name in args:
                sq = getattr(db_map, name, None)
                if sq is None:
                    continue
                process_row = _get_row_processor(name)
                data[name] = [process_row(x._asdict()) for x in db_map.query(sq)]
        return data

    def import_data(self, data, comment):
        """Imports data and commit.

        Args:
            data (dict)
            comment (str)
        Returns:
            list or dict: list of import errors, if successful. Dictionary {"error": "msg"}, otherwise.
        """
        with self._closing_db_map(create=True) as (db_map, error):
            if error:
                return {"error": str(error)}
            count, errors = import_data(db_map, **data)
            if count:
                try:
                    db_map.commit_session(comment)
                except DBAPIError:
                    db_map.rollback_session()
        return [err.msg for err in errors]


_db_maps = {}


class PersistentDBHandler(DBHandler):
    """
    Helper class to reuse DatabaseMapping objects for the same url.
    Used by SpineOpt unit-tests.
    """

    def _make_db_map(self, create=True):
        if self._db_url not in _db_maps:
            _db_maps[self._db_url] = DatabaseMapping(self._db_url, upgrade=self._upgrade, create=create)
        return _db_maps[self._db_url], None

    @contextmanager
    def _closing_db_map(self, create=True):
        db_map, error = self._make_db_map(create=create)
        try:
            yield db_map, error
        finally:
            pass


def close_persistent_db_map(db_url):
    db_map = _db_maps.pop(db_url, None)
    if db_map is not None:
        db_map.connection.close()


class DBRequestHandler(ReceiveAllMixing, DBHandler, socketserver.BaseRequestHandler):
    """
    The request handler class for our server.
    """

    def handle(self):
        data = self._recvall()
        request, args = json.loads(data)
        handler = {"get_data": self.get_data, "import_data": self.import_data, "get_db_url": self.get_db_url}.get(
            request
        )
        if handler is None:
            return
        response = handler(*args)
        if response is not None:
            self.request.sendall(bytes(json.dumps(response) + self._EOM, self._ENCODING))


class SpineDBServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


_servers = {}


def start_spine_db_server(db_url, upgrade=False):
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
    server = _servers[server_url] = SpineDBServer(
        (host, port), lambda *args, **kwargs: DBRequestHandler(db_url, upgrade, *args, **kwargs)
    )
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    return server_url


def shutdown_spine_db_server(server_url):
    server = _servers.pop(server_url, None)
    if server is not None:
        server.shutdown()
        server.server_close()


def _shutdown_servers():
    for server in _servers.values():
        server.shutdown()
        server.server_close()


atexit.register(_shutdown_servers)
