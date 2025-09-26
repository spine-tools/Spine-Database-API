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
This module provides a mechanism to create a socket server interface to a Spine DB.
The server exposes most of the functionality of :class:`~spinedb_api.db_mapping.DatabaseMapping`,
and can eventually remove the ``spinedb_api`` requirement (and the Python requirement altogether)
from third-party applications that want to interact with Spine DBs. (Of course, they would need to have
access to sockets instead.)

Typically, you would start the server in a background Python process by specifying the URL of the target Spine DB,
getting back the URL where the server is listening.
You can then use that URL in any number of instances of your application that would connect to the server
via a socket and then send requests to retrieve or modify the data in the DB.

Requests to the server must be encoded using JSON.
Each request must be a JSON array with the following elements:

#. A JSON string with one of the available request names:
   ``get_db_url``, ``import_data``, ``export_data``, ``query``, ``filtered_query``, ``apply_filters``,
   ``clear_filters``, ``call_method``, ``db_checkin``, ``db_checkout``.
#. A JSON array with positional arguments to the request.
#. A JSON object with keyword arguments to the request.
#. A JSON integer indicating the version of the server you want to talk to.

The positional and keyword arguments to the different requests are documented
in the :class:`~spinedb_api.spine_db_client.SpineDBClient` class
(just look for a member function named after the request).

The point of the server version is to allow client developers to adapt to changes in the Spine DB server API.
Say we update ``spinedb_api`` and change the signature of one of the requests - in this case, we will
also bump the current server version to the next integer.
If you then upgrade your ``spinedb_api`` installation but not your client, the server will be able to respond
with an error message saying that you need to update your client.
The current server version can be queried by calling :func:`get_current_server_version`.

The order in which multiple servers should write to the same DB can also be controlled using DB servers.
This is particularly useful in high-concurrency scenarios.

The server is started using :func:`closing_spine_db_server`.
To control the order of writing you need to provide a queue, that you would obtain by calling :func:`db_server_manager`.

The below example illustrates most of the functionality of the module.
We create two DB servers targeting the same DB, and set the second to write before the first
(via the ``ordering`` argument to :func:`closing_spine_db_server`).
Then we spawn two threads that connect to those two servers and import an entity class.
We make sure to call :meth:`~spinedb_api.spine_db_client.SpineDBClient.db_checkin` before importing,
and :meth:`~spinedb_api.spine_db_client.SpineDBClient.db_checkout` after so the order of writing is respected.
When the first thread attemps to write to the DB, it hangs because the second one hasn't written yet.
Only after the second writes, the first one also writes and the program finishes::

    import threading
    from spinedb_api.spine_db_server import db_server_manager, closing_spine_db_server
    from spinedb_api.spine_db_client import SpineDBClient
    from spinedb_api.db_mapping import DatabaseMapping


    def _import_entity_class(server_url, class_name):
        client = SpineDBClient.from_server_url(server_url)
        client.db_checkin()
        _answer = client.import_data({"entity_classes": [(class_name, ())]}, f"Import {class_name}")
        client.db_checkout()


    db_url = 'sqlite:///somedb.sqlite'
    with db_server_manager() as mngr_queue:
        first_ordering = {"id": "second_before_first", "current": "first", "precursors": {"second"}, "part_count": 1}
        second_ordering = {"id": "second_before_first", "current": "second", "precursors": set(), "part_count": 1}
        with closing_spine_db_server(
            db_url, server_manager_queue=mngr_queue, ordering=first_ordering
        ) as first_server_url:
            with closing_spine_db_server(
                db_url, server_manager_queue=mngr_queue, ordering=second_ordering
            ) as second_server_url:
                t1 = threading.Thread(target=_import_entity_class, args=(first_server_url, "monkey"))
                t2 = threading.Thread(target=_import_entity_class, args=(second_server_url, "donkey"))
                t1.start()
                with DatabaseMapping(db_url) as db_map:
                    assert db_map.get_items("entity_class") == []  # Nothing written yet
                t2.start()
                t1.join()
                t2.join()

    with DatabaseMapping(db_url) as db_map:
        assert [x["name"] for x in db_map.get_items("entity_class")] == ["donkey", "monkey"]
"""

import atexit
from collections.abc import Hashable, Iterator
from contextlib import contextmanager
import multiprocessing as mp
from multiprocessing.queues import Queue as MPQueue
from queue import Queue
import socketserver
import threading
import time
import traceback
from typing import ClassVar, Literal, Optional, TypedDict
from urllib.parse import urlunsplit
import uuid
from sqlalchemy.exc import DBAPIError
from spinedb_api import __version__ as spinedb_api_version
from .db_mapping import DatabaseMapping
from .export_functions import export_data
from .filters.alternative_filter import alternative_filter_config
from .filters.scenario_filter import scenario_filter_config
from .filters.tools import apply_filter_stack, clear_filter_configs
from .import_functions import import_data
from .parameter_value import dump_db_value
from .server_client_helpers import ReceiveAllMixing, decode, encode
from .spine_db_client import SpineDBClient

_current_server_version = 8


class OrderingDict(TypedDict):
    """Required keys:

    - "id": an identifier for the ordering, shared by all the servers in the ordering.
    - "current": an identifier for this server within the ordering.
    - "precursors": a set of identifiers of other servers that must have checked out from the DB before this one can check in.
    - "part_count": the number of times this server needs to check out from the DB before their successors can check in.
    """

    id: Hashable
    current: Hashable
    precursors: set[Hashable]
    part_count: int


def get_current_server_version() -> int:
    """Returns the current client version.

    Returns:
        current client version
    """
    return _current_server_version


class _DeepCopyableQueue(MPQueue):
    """A Queue that supports deepcopy so it can be passed around within resources metadata at engine level."""

    def __init__(self, *args, **kwargs):
        ctx = mp.get_context()
        super().__init__(*args, **kwargs, ctx=ctx)

    def __deepcopy__(self, _memo):
        return self


class _DBServerManager:
    """Enables synchronization between DB servers.
    All the associated DB servers have access to the _queue attribute which they can use to issue requests,
    typically related to checking in and out of DBs.
    """

    _SHUTDOWN: ClassVar[Literal["shutdown"]] = "shutdown"
    _CHECKOUT_COMPLETE: ClassVar[Literal["checkout_complete"]] = "checkout_complete"

    def __init__(self):
        super().__init__()
        self._servers = {}
        self._checkouts = {}
        self._waiters = {}
        self._commit_locks = {}
        self._queue = _DeepCopyableQueue()
        self._process = mp.Process(target=self._do_work)
        self._process.start()

    def _get_commit_lock(self, db_url):
        clean_url = clear_filter_configs(db_url)
        return self._commit_locks.setdefault(clean_url, threading.Lock())

    def shutdown(self):
        self._queue.put(self._SHUTDOWN)
        self._process.join()

    @property
    def queue(self):
        return self._queue

    @property
    def _handlers(self):
        return {
            "start_server": self._start_server,
            "shutdown_server": self._shutdown_server,
            "db_checkin": self._db_checkin,
            "db_checkout": self._db_checkout,
            "quick_db_checkout": self._quick_db_checkout,
            "cancel_db_checkout": self._cancel_db_checkout,
        }

    def _do_work(self):
        while True:
            msg = self._queue.get()
            if msg == self._SHUTDOWN:
                break
            output_queue, request, args, kwargs = msg
            handler = self._handlers[request]
            result = handler(*args, **kwargs)
            output_queue.put(result)
        for server_address in list(self._servers):
            self._shutdown_server(server_address)

    def _start_server(self, db_url, upgrade, memory, ordering):
        host = "127.0.0.1"
        commit_lock = self._get_commit_lock(db_url)
        while True:
            with socketserver.TCPServer((host, 0), None) as s:
                port = s.server_address[1]
            try:
                server = SpineDBServer(
                    db_url, upgrade, memory, commit_lock, self._queue, ordering, (host, port), DBRequestHandler
                )
                break
            except OSError:
                # [Errno 98] Address already in use
                time.sleep(0.02)
        self._servers[server.server_address] = server
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        return server.server_address

    def _shutdown_server(self, server_address):
        server = self._servers.pop(server_address, None)
        if server is None:
            return False
        server.close_db_map()
        server.shutdown()
        server.server_close()
        return True

    def _db_checkin(self, server_address):
        server = self._servers.get(server_address)
        if not server:
            return
        ordering = server.ordering
        checkouts = self._checkouts.get(ordering["id"], {})
        full_checkouts = set(x for x, count in checkouts.items() if count == self._CHECKOUT_COMPLETE)
        precursors = ordering["precursors"]
        if precursors <= full_checkouts:
            return
        event = mp.Manager().Event()
        self._waiters.setdefault(ordering["id"], {})[event] = precursors
        return event

    def _db_checkout(self, server_address):
        server = self._servers.get(server_address)
        if not server:
            return
        ordering = server.ordering
        self._quick_db_checkout(ordering)

    def _quick_db_checkout(self, ordering):
        current = ordering["current"]
        checkouts = self._checkouts.setdefault(ordering["id"], {})
        if current not in checkouts:
            checkouts[current] = 1
        elif checkouts[current] != self._CHECKOUT_COMPLETE:
            checkouts[current] += 1
        if checkouts[current] == ordering["part_count"]:
            checkouts[current] = self._CHECKOUT_COMPLETE
        full_checkouts = set(x for x, count in checkouts.items() if count == self._CHECKOUT_COMPLETE)
        waiters = self._waiters.get(ordering["id"], {})
        done = [event for event, precursors in waiters.items() if precursors <= full_checkouts]
        for event in done:
            del waiters[event]
            event.set()

    def _cancel_db_checkout(self, server_address):
        server = self._servers.get(server_address)
        if not server:
            return
        ordering = server.ordering
        checkouts = self._checkouts.get(ordering["id"], {})
        checkouts.pop(ordering["current"], None)


def _run_request_on_manager(request, server_manager_queue, *args, **kwargs):
    with mp.Manager() as manager:
        output_queue = manager.Queue()
        server_manager_queue.put((output_queue, request, args, kwargs))
        return output_queue.get()


def start_spine_db_server(server_manager_queue, db_url, upgrade: bool = False, memory: bool = False, ordering=None):
    return _run_request_on_manager("start_server", server_manager_queue, db_url, upgrade, memory, ordering)


def shutdown_spine_db_server(server_manager_queue, server_address):
    return _run_request_on_manager("shutdown_server", server_manager_queue, server_address)


def db_checkin(server_manager_queue, server_address):
    event = _run_request_on_manager("db_checkin", server_manager_queue, server_address)
    if event:
        event.wait()


def db_checkout(server_manager_queue, server_address):
    return _run_request_on_manager("db_checkout", server_manager_queue, server_address)


def quick_db_checkout(server_manager_queue, ordering):
    return _run_request_on_manager("quick_db_checkout", server_manager_queue, ordering)


def cancel_db_checkout(server_manager_queue, server_address):
    return _run_request_on_manager("cancel_db_checkout", server_manager_queue, server_address)


def _parse_value(v, type_):
    return (v, type_)


def _unparse_value(value_and_type):
    if isinstance(value_and_type, (tuple, list)) and len(value_and_type) == 2:
        value, type_ = value_and_type
        if value is None or (isinstance(value, bytes) and (isinstance(type_, str) or type_ is None)):
            # Tuple of value and type ready to go
            return value, type_
    # JSON object
    return dump_db_value(value_and_type)


class SpineDBServerBase:
    """Implements the interface between the server and the DB.

    Since server requests might come from different threads, and SQLite objects can only be used
    from the thread they were created, here we need to use a dedicated thread to hold and manipulate
    our DatabaseMapping."""

    _CLOSE: ClassVar[Literal["close"]] = "close"

    def __init__(self, db_url, upgrade, memory, commit_lock, manager_queue, ordering, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manager_queue = manager_queue
        self.ordering = ordering
        self._db_map = None
        self._closed = False
        self._lock = threading.Lock()
        self._in_queue = Queue()
        self._out_queue = Queue()
        self._thread = threading.Thread(target=lambda: self._do_work(db_url, upgrade, memory, commit_lock))
        self._thread.start()
        error = self._out_queue.get()
        if isinstance(error, Exception):
            raise error

    @property
    def db_url(self):
        return str(self._db_map.db_url)

    def close_db_map(self):
        if not self._closed:
            self._closed = True
            self._db_map.close()
            self._in_queue.put(self._CLOSE)
            self._thread.join()

    def _do_work(self, db_url, upgrade, memory, commit_lock):
        try:
            self._db_map = DatabaseMapping(db_url, upgrade=upgrade, memory=memory, commit_lock=commit_lock, create=True)
            self._out_queue.put(None)
        except Exception as error:  # pylint: disable=broad-except
            self._out_queue.put(error)
            return
        while True:
            input_ = self._in_queue.get()
            if input_ == self._CLOSE:
                break
            request, args, kwargs = input_
            handler = {
                "query": self._do_query,
                "filtered_query": self._do_filtered_query,
                "import_data": self._do_import_data,
                "export_data": self._do_export_data,
                "call_method": self._do_call_method,
                "apply_filters": self._do_apply_filters,
                "clear_filters": self._do_clear_filters,
            }[request]
            with self._db_map:
                result = handler(*args, **kwargs)
            self._out_queue.put(result)

    def run_request(self, request, args, kwargs):
        with self._lock:
            self._in_queue.put((request, args, kwargs))
            return self._out_queue.get()

    def _do_query(self, *args):
        result = {}
        for sq_name in args:
            sq = getattr(self._db_map, sq_name, None)
            if sq is None:
                continue
            result[sq_name] = [x._asdict() for x in self._db_map.query(sq)]
        return {"result": result}

    def _do_filtered_query(self, **kwargs):
        result = {}
        for sq_name, filters in kwargs.items():
            sq = getattr(self._db_map, sq_name, None)
            if sq is None:
                continue
            qry = self._db_map.query(sq)
            for field, value in filters.items():
                qry = qry.filter_by(**{field: value})
            result[sq_name] = [x._asdict() for x in qry]
        return {"result": result}

    def _do_import_data(self, data, comment):
        count, errors = import_data(self._db_map, unparse_value=_unparse_value, **data)
        if count and comment:
            try:
                self._db_map.commit_session(comment)
            except DBAPIError:
                self._db_map.rollback_session()
        return {"result": (count, errors)}

    def _do_export_data(self, **kwargs):
        return {"result": export_data(self._db_map, parse_value=_parse_value, **kwargs)}

    def _do_call_method(self, method_name, *args, **kwargs):
        try:
            method = getattr(self._db_map, method_name)
            result = method(*args, **kwargs)
            return {"result": result}
        except Exception as err:
            return {"error": str(err)}

    def _do_clear_filters(self):
        self._db_map.restore_entity_sq_maker()
        self._db_map.restore_entity_element_sq_maker()
        self._db_map.restore_entity_location_sq_maker()
        self._db_map.restore_entity_class_sq_maker()
        self._db_map.restore_entity_alternative_sq_maker()
        self._db_map.restore_entity_group_sq_maker()
        self._db_map.restore_parameter_definition_sq_maker()
        self._db_map.restore_parameter_value_sq_maker()
        self._db_map.restore_alternative_sq_maker()
        self._db_map.restore_scenario_sq_maker()
        self._db_map.restore_scenario_alternative_sq_maker()
        self._db_map.filter_configs.clear()
        return {"result": True}

    def _do_apply_filters(self, configs):
        try:
            apply_filter_stack(self._db_map, configs)
            return {"result": True}
        except Exception as error:  # pylint: disable=broad-except
            return {"error": str(error)}


class SpineDBServer(SpineDBServerBase, socketserver.TCPServer):
    """A socket server for accessing and manipulating a Spine DB."""


class HandleDBRequestMixin:
    def get_db_url(self):
        """
        Returns:
            str: The underlying db url
        """
        return self.server.db_url

    def _run_request_on_server(self, request, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}
        return self.server.run_request(request, args, kwargs)

    def query(self, *args):
        """
        Runs queries.

        Returns:
            dict: where result is a dict from subquery name to a list of items from thay subquery, if successful.
        """
        return self._run_request_on_server("query", args=args)

    def filtered_query(self, **kwargs):
        """
        Runs queries with filters.

        Returns:
            dict: where result is a dict from subquery name to a list of items from thay subquery, if successful.
        """
        return self._run_request_on_server("filtered_query", kwargs=kwargs)

    def import_data(self, data, comment):
        """Imports data and commit.

        Args:
            data (dict)
            comment (str)
        Returns:
            dict: where result is a list of import errors, if successful.
        """
        return self._run_request_on_server("import_data", args=(data, comment))

    def export_data(self, **kwargs):
        """Exports data.

        Returns:
            dict: where result is the data exported from the db
        """
        return self._run_request_on_server("export_data", kwargs=kwargs)

    def call_method(self, method_name, *args, **kwargs):
        """Calls a method from the DatabaseMapping class.

        Args:
            method_name (str): the method name
            args: positional arguments passed to the method call
            kwargs: keyword arguments passed to the method call

        Returns:
            dict: where result is the return value of the method
        """
        return self._run_request_on_server("call_method", args=(method_name, *args), kwargs=kwargs)

    def apply_filters(self, filters):
        obsolete = ("tool",)
        configs = [
            {"scenario": scenario_filter_config, "alternatives": alternative_filter_config}[key](value)
            for key, value in filters.items()
            if key not in obsolete
        ]
        return self._run_request_on_server("apply_filters", args=(configs,))

    def clear_filters(self):
        return self._run_request_on_server("clear_filters")

    def db_checkin(self):
        db_checkin(self.server_manager_queue, self.server_address)
        return {"result": True}

    def db_checkout(self):
        db_checkout(self.server_manager_queue, self.server_address)
        return {"result": True}

    def cancel_db_checkout(self):
        cancel_db_checkout(self.server_manager_queue, self.server_address)
        return {"result": True}

    def _get_response(self, request):
        request, *extras = decode(request)
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
        if client_version < _current_server_version:
            return {"error": 1, "result": _current_server_version}
        handler = {
            "query": self.query,
            "filtered_query": self.filtered_query,
            "import_data": self.import_data,
            "export_data": self.export_data,
            "call_method": self.call_method,
            "apply_filters": self.apply_filters,
            "clear_filters": self.clear_filters,
            "db_checkin": self.db_checkin,
            "db_checkout": self.db_checkout,
            "cancel_db_checkout": self.cancel_db_checkout,
        }.get(request)
        if handler is None:
            return {"error": f"invalid request '{request}'"}
        try:
            return handler(*args, **kwargs)
        except Exception:  # pylint: disable=broad-except
            return {"error": traceback.format_exc()}

    def handle_request(self, request):
        response = self._get_response(request)
        return encode(response)


class DBHandler(HandleDBRequestMixin):
    """Enables manipulating a DB by sending the same requests one would send to a DB server.
    This allows clients to use a common interface to communicate with DBs without worrying if the communication
    is going to take place with or without a server.
    At the moment it's used by SpineInterface to run unit-tests.
    """

    def __init__(self, db_url, upgrade=False, memory=False):
        self.server = SpineDBServerBase(db_url, upgrade, memory, None, None, None)
        atexit.register(self.close)

    def close(self):
        self.server.close_db_map()


class DBRequestHandler(ReceiveAllMixing, HandleDBRequestMixin, socketserver.BaseRequestHandler):
    """Handles requests to a DB server."""

    @property
    def server_address(self):
        return self.server.server_address

    @property
    def server_manager_queue(self):
        return self.server.manager_queue

    def handle(self):
        request = self._recvall()
        response = self.handle_request(request)
        self.request.sendall(response + bytes(self._EOT, self._ENCODING))


@contextmanager
def db_server_manager() -> Iterator[MPQueue]:
    """Creates a DB server manager that can be used to control the order in which different servers
    write to the same DB.

    Yields:
        a queue that can be passed to :func:`.closing_spine_db_server`
        in order to control write order.
    """
    mngr = _DBServerManager()
    try:
        yield mngr.queue
    finally:
        mngr.shutdown()


@contextmanager
def closing_spine_db_server(
    db_url: str,
    upgrade: bool = False,
    memory: bool = False,
    ordering: Optional[OrderingDict] = None,
    server_manager_queue: Optional[MPQueue] = None,
):
    """Creates a Spine DB server.

    Args:
        db_url: the URL of a Spine DB.
        upgrade: Whether to upgrade the DB to the last revision.
        memory: Whether to use an in-memory database together with a persistent connection.
        server_manager_queue: A queue that can be used to control order of writing.
            Only needed if you also specify `ordering` below.
        ordering: A dictionary specifying an ordering to be followed by multiple concurrent servers
            writing to the same DB.

    Yields:
        server url
    """
    if server_manager_queue is None:
        mngr = _DBServerManager()
        server_manager_queue = mngr.queue
    else:
        mngr = None
    if ordering is None:
        ordering = {
            "id": 0,
            "current": 0,
            "precursors": set(),
            "part_count": 0,
        }
    server_address = start_spine_db_server(server_manager_queue, db_url, memory=memory, ordering=ordering)
    host, port = server_address
    try:
        yield urlunsplit(("http", f"{host}:{port}", "", "", ""))
    finally:
        shutdown_spine_db_server(server_manager_queue, server_address)
        if mngr is not None:
            mngr.shutdown()
