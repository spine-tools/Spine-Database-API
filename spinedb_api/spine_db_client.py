######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains the SpineDBClient class.

"""

from urllib.parse import urlparse
import socket
from sqlalchemy.engine.url import URL
from .server_client_helpers import ReceiveAllMixing, encode, decode

client_version = 6


class SpineDBClient(ReceiveAllMixing):
    def __init__(self, server_address):
        """
        Args:
            server_address (tuple(str,int)): hostname and port
        """
        self._server_address = server_address
        self.request = None

    @classmethod
    def from_server_url(cls, url):
        parsed = urlparse(url)
        if parsed.scheme != "http":
            raise ValueError(f"unable to create client, invalid server url {url}")
        return cls((parsed.hostname, parsed.port))

    def get_db_url(self):
        """
        Returns:
            str: The underlying db url from the server
        """
        return self._send("get_db_url")

    def db_checkin(self):
        return self._send("db_checkin")

    def db_checkout(self):
        return self._send("db_checkout")

    def cancel_db_checkout(self):
        return self._send("cancel_db_checkout")

    def import_data(self, data, comment):
        return self._send("import_data", args=(data, comment))

    def export_data(self, **kwargs):
        return self._send("export_data", kwargs=kwargs)

    def call_method(self, method_name, *args, **kwargs):
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
    """Returns the underlying db url associated with the given url, if it's a server url.
    Otherwise, it assumes it's the url of DB and returns it unaltered.
    Used by ``DatabaseMappingBase()``.

    Args:
        url (str, URL): a url, either from a Spine DB or from a Spine DB server.

    Returns:
        str
    """
    if isinstance(url, URL):
        return url
    parsed = urlparse(url)
    if parsed.scheme != "http":
        return url
    return SpineDBClient((parsed.hostname, parsed.port)).get_db_url()
