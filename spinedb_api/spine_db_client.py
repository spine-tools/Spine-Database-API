######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
This module defines the :class:`SpineDBClient` class.
"""

from urllib.parse import urlparse
import socket
from sqlalchemy.engine.url import URL
from .server_client_helpers import ReceiveAllMixing, encode, decode

client_version = 6


class SpineDBClient(ReceiveAllMixing):
    def __init__(self, server_address):
        """Enables sending requests to a Spine DB server.

        Args:
            server_address (tuple(str,int)): the hostname and port where the server is listening.
        """
        self._server_address = server_address
        self.request = None

    @classmethod
    def from_server_url(cls, url):
        """Creates a client from a server's URL.

        Args:
            url (str, URL): the URL where the server is listening.
        """
        parsed = urlparse(url)
        if parsed.scheme != "http":
            raise ValueError(f"unable to create client, invalid server url {url}")
        return cls((parsed.hostname, parsed.port))

    def get_db_url(self):
        """Returns the URL of the target Spine DB - the one the server is set to communicate with.

        Returns:
            str
        """
        return self._send("get_db_url")

    def db_checkin(self):
        """Blocks until all the servers that need to write to the same DB before this one
        have reported all their writes."""
        return self._send("db_checkin")

    def db_checkout(self):
        """Reports one write for this server."""
        return self._send("db_checkout")

    def cancel_db_checkout(self):
        """Reverts the last write report for this server."""
        return self._send("cancel_db_checkout")

    def import_data(self, data, comment):
        """Imports data to the DB using :func:`~spinedb_api.import_functions.import_data` and commits the changes.

        Args:
            data (dict): to be splatted into keyword arguments to :func:`~spinedb_api.import_functions.import_data`
            comment (str): a commit message.
        """
        return self._send("import_data", args=(data, comment))

    def export_data(self, **kwargs):
        """Exports data from the DB using :func:`~spinedb_api.export_functions.export_data`.

        Args:
            **kwargs: keyword arguments passed to :func:`~spinedb_api.import_functions.export_data`
        """
        return self._send("export_data", kwargs=kwargs)

    def call_method(self, method_name, *args, **kwargs):
        """Calls a method from :class:`~spinedb_api.db_mapping.DatabaseMapping`.

        Args:
            method_name (str): the name of the method to call.
            *args: positional arguments passed to the method call.
            **kwargs: keyword arguments passed to the method call.
        """
        return self._send("call_method", args=(method_name, *args), kwargs=kwargs)

    def open_db_map(self, db_url, upgrade, memory):
        return self._send("open_db_map", args=(db_url, upgrade, memory))

    def close_db_map(self):
        return self._send("close_db_map")

    def _send(self, request, args=None, kwargs=None, receive=True):
        """
        Sends a request to the server with the given arguments.

        Args:
            request (str): One of the supported engine server requests
            args: Request arguments
            receive (bool, optional): If True (the default) also receives the response and returns it.

        Returns:
            str or NoneType: response, or None if receive is False
        """
        args = () if args is None else args
        kwargs = {} if kwargs is None else kwargs
        msg = encode((request, args, kwargs, client_version))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.request:
            self.request.connect(self._server_address)
            self.request.sendall(msg + self._BEOT)
            if receive:
                response = self._recvall()
                return decode(response)


def get_db_url_from_server(url):
    if isinstance(url, URL):
        return url
    parsed = urlparse(url)
    if parsed.scheme != "http":
        return url
    return SpineDBClient((parsed.hostname, parsed.port)).get_db_url()
