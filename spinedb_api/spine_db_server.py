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


_db_maps = {}


class DBRequestHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.
    """

    _ENCODING = "utf-8"

    def __init__(self, db_url, upgrade, *args, **kwargs):
        self._db_url = db_url
        self._upgrade = upgrade
        super().__init__(*args, **kwargs)

    @contextmanager
    def _closing_db_map(self):
        db_map = DatabaseMapping(self._db_url, upgrade=self._upgrade)
        try:
            yield db_map
        finally:
            db_map.connection.close()

    def _get_data(self, *args):
        """
        Returns:
            dict: mapping sq names in args to a list of items from thay subquery
        """
        data = {}
        with self._closing_db_map() as db_map:
            for name in args:
                sq = getattr(db_map, name, None)
                if sq is None:
                    continue
                data[name] = [x._asdict() for x in db_map.query(sq)]
        return data

    def _import_data(self, data, comment):
        """Imports data and commit.

        Args:
            data (dict)
            comment (str)
        Returns:
            int: Import count
            list: Errors
        """
        with self._closing_db_map() as db_map:
            count, errors = import_data(db_map, **data)
            if count:
                try:
                    db_map.commit_session(comment)
                except DBAPIError:
                    db_map.rollback_session()
        return errors

    def handle(self):
        data = self._recvall()
        request, args = json.loads(data)
        handler = {"get_data": self._get_data, "import_data": self._import_data}.get(request)
        if handler is None:
            return
        response = handler(*args)
        if response is not None:
            self.request.sendall(bytes(json.dumps(response), self._ENCODING))

    def _recvall(self):
        """
        Receives and returns all data in the request.

        Returns:
            str
        """
        BUFF_SIZE = 4096
        fragments = []
        while True:
            chunk = str(self.request.recv(BUFF_SIZE), self._ENCODING)
            if chunk[-1] == "\0":
                fragments.append(chunk[:-1])
                break
            fragments.append(chunk)
        return "".join(fragments)


class SpineDBServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


_servers = {}


def start_spine_db_server(db_url, upgrade=False):
    host = "localhost"
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
