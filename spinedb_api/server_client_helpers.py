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

import json
from .exception import SpineDBAPIError
from .db_mapping_base import PublicItem
from .temp_id import TempId

# Encode decode server messages
_START_OF_TAIL = "\u001f"  # Unit separator
_START_OF_ADDRESS = "\u0091"  # Private Use 1
_ADDRESS_SEP = ":"


class ReceiveAllMixing:
    _ENCODING = "utf-8"
    _BUFF_SIZE = 4096
    _EOT = "\u0004"  # End of transmission
    _BEOT = _EOT.encode(_ENCODING)
    """End of message character"""

    def _recvall(self):
        """
        Receives and returns all data in the request.

        Returns:
            str
        """
        fragments = []
        while True:
            chunk = self.request.recv(self._BUFF_SIZE)
            fragments.append(chunk)
            if chunk.endswith(self._BEOT):
                break
        return b"".join(fragments)[:-1]


class _TailJSONEncoder(json.JSONEncoder):
    """
    A custom JSON encoder that accummulates bytes objects into a tail.
    Each bytes object is encoded as a string pointing to the address in the tail.
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
        if isinstance(o, PublicItem):
            return o._extended()
        if isinstance(o, TempId):
            return o.private_id
        return super().default(o)

    @property
    def tail(self):
        return b"".join(self._tail_parts)


def encode(o):
    """
    Encodes given object into a message to be sent via a socket, with the following structure:

        body | start of tail character | tail

    The body is obtained by JSON-encoding the argument, while replacing all `bytes` objects by addresses in the tail.
    The tail is computed by concatenating all `bytes` objects in the argument.
    See class:`_TailJSONEncoder`.

    Args:
        o (any): A Python object to encode.

    Returns:
        bytes: Encoded message.
    """
    encoder = _TailJSONEncoder()
    s = encoder.encode(o)
    return s.encode() + _START_OF_TAIL.encode() + encoder.tail


def decode(b):
    """
    Decodes given message received via a socket into a Python object.
    The message must have the following structure:

        body | start of tail character | tail

    The result is obtained by JSON-decoding the body, and then replacing all the addresses with the referred `bytes`
    from the tail.

    Args:
        b (bytes): A message to decode.

    Returns:
        any: Decoded object.
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
