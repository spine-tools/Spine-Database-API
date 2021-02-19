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
Contains the SpineDBClient class.

:authors: M. Marin (KTH)
:date:   19.2.2020
"""

from urllib.parse import urlparse
import socket
import json
from .helpers import ReceiveAllMixing


class SpineDBClient(ReceiveAllMixing):
    def __init__(self, server_address):
        """
        Args:
            server_address (tuple(str,int)): hostname and port
        """
        self._server_address = server_address
        self.request = None

    def get_db_url(self):
        """
        Returns:
            str: The underlying db url from the server
        """
        return self._send("get_db_url")

    def _send(self, request, *args, receive=True):
        """
        Sends a request to the server with the given arguments.

        Args:
            request (str): One of the supported engine server requests
            args: Request arguments
            receive (bool, optional): If True (the default) also receives the response and returns it.

        Returns:
            str or NoneType: response, or None if receive is False
        """
        msg = json.dumps((request, args))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.request:
            self.request.connect(self._server_address)
            self.request.sendall(bytes(msg + self._EOM, self._ENCODING))
            if receive:
                response = self._recvall()
                return json.loads(response)


def get_db_url_from_server(url):
    """Returns the underlying db url associated with the given url, if it's a server url.
    Otherwise, it assumes it's the url of DB and returns it unaltered.
    Used by ``DatabaseMappingBase()``.

    Args:
        url (str): a url, either from a Spine DB or from a Spine DB server.

    Returns:
        str
    """
    parsed = urlparse(url)
    if parsed.scheme != "http":
        return url
    return SpineDBClient((parsed.hostname, parsed.port)).get_db_url()
