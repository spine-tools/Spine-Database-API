######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains the SpineDBServer class.

"""

from urllib.parse import urlunsplit
from contextlib import contextmanager
import socketserver
import multiprocessing as mp
from multiprocessing.queues import Queue as MPQueue
import threading
import atexit
import traceback
import time
import uuid
from queue import Queue
from sqlalchemy.exc import DBAPIError
from spinedb_api import __version__ as spinedb_api_version
from .db_mapping import DatabaseMapping
from .import_functions import import_data
from .export_functions import export_data
from .parameter_value import dump_db_value
from .server_client_helpers import ReceiveAllMixing, encode, decode
from .filters.scenario_filter import scenario_filter_config
from .filters.tool_filter import tool_filter_config
from .filters.alternative_filter import alternative_filter_config
from .filters.tools import apply_filter_stack
from .spine_db_client import SpineDBClient

_required_client_version = 6


def _parse_value(v, value_type=None):
    return (v, value_type)


def _unparse_value(value_and_type):
    if isinstance(value_and_type, (tuple, list)) and len(value_and_type) == 2:
        value, type_ = value_and_type
        if value is None or (isinstance(value, bytes) and (isinstance(type_, str) or type_ is None)):
            # Tuple of value and type ready to go
            return value, type_
    # JSON object
    return dump_db_value(value_and_type)


class SpineDBServer(socketserver.TCPServer):
    def __init__(self, manager_queue, ordering, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manager_queue = manager_queue
        self.ordering = ordering


class _DeepCopyableQueue(MPQueue):
    def __init__(self, *args, **kwargs):
        ctx = mp.get_context()
        super().__init__(*args, **kwargs, ctx=ctx)

    def __deepcopy__(self, _memo):
        return self


class _DBServerManager:
    _SHUTDOWN = "shutdown"
    _CHECKOUT_COMPLETE = "checkout_complete"

    def __init__(self):
        super().__init__()
        self._servers = {}
        self._checkouts = {}
        self._waiters = {}
        self._queue = _DeepCopyableQueue()
        self._process = mp.Process(target=self._do_work)
        self._process.start()

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
            "shutdown_servers": self._shutdown_servers,
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
        while True:
            with socketserver.TCPServer((host, 0), None) as s:
                port = s.server_address[1]
            try:
                server = SpineDBServer(self._queue, ordering, (host, port), DBRequestHandler)
                break
            except OSError:
                # [Errno 98] Address already in use
                time.sleep(0.02)
        self._servers[server.server_address] = server
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        error = SpineDBClient(server.server_address).open_db_map(db_url, upgrade, memory).get("error")
        if error:
            raise RuntimeError(error)
        return server.server_address

    def _shutdown_server(self, server_address):
        server = self._servers.pop(server_address, None)
        if server is None:
            return False
        SpineDBClient(server.server_address).close_db_map()
        server.shutdown()
        server.server_close()
        return True

    def _shutdown_servers(self):
        return all(self._shutdown_server(server_address) for server_address in list(self._servers))

    def _db_checkin(self, server_address):
        server = self._servers.get(server_address)
        if not server:
            return
        ordering = server.ordering
        checkouts = self._checkouts.get(ordering["id"], dict())
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
        checkouts = self._checkouts.setdefault(ordering["id"], dict())
        if current not in checkouts:
            checkouts[current] = 1
        elif checkouts[current] != self._CHECKOUT_COMPLETE:
            checkouts[current] += 1
        if checkouts[current] == ordering["part_count"]:
            checkouts[current] = self._CHECKOUT_COMPLETE
        full_checkouts = set(x for x, count in checkouts.items() if count == self._CHECKOUT_COMPLETE)
        waiters = self._waiters.get(ordering["id"], dict())
        done = [event for event, precursors in waiters.items() if precursors <= full_checkouts]
        for event in done:
            del waiters[event]
            event.set()

    def _cancel_db_checkout(self, server_address):
        server = self._servers.get(server_address)
        if not server:
            return
        ordering = server.ordering
        checkouts = self._checkouts.get(ordering["id"], dict())
        checkouts.pop(ordering["current"], None)


class _ManagerRequestHandler:
    def __init__(self, mngr_queue):
        self._mngr_queue = mngr_queue

    def _run_request(self, request, *args, **kwargs):
        with mp.Manager() as manager:
            output_queue = manager.Queue()
            self._mngr_queue.put((output_queue, request, args, kwargs))
            return output_queue.get()

    def start_server(self, db_url, upgrade, memory, ordering):
        return self._run_request("start_server", db_url, upgrade, memory, ordering)

    def shutdown_server(self, server_address):
        return self._run_request("shutdown_server", server_address)

    def shutdown_servers(self):
        return self._run_request("shutdown_servers")

    def register_ordering(self, server_address, ordering):
        return self._run_request("register_ordering", server_address, ordering)

    def db_checkin(self, server_address):
        event = self._run_request("db_checkin", server_address)
        if event:
            event.wait()

    def db_checkout(self, server_address):
        return self._run_request("db_checkout", server_address)

    def quick_db_checkout(self, ordering):
        return self._run_request("quick_db_checkout", ordering)

    def cancel_db_checkout(self, server_address):
        return self._run_request("cancel_db_checkout", server_address)


class _DBWorker:
    _CLOSE = "close"

    def __init__(self, db_url, upgrade, memory, create=True):
        self._db_url = db_url
        self._upgrade = upgrade
        self._memory = memory
        self._create = create
        self._db_map = None
        self._lock = threading.Lock()
        self._in_queue = Queue()
        self._out_queue = Queue()
        self._thread = threading.Thread(target=self._do_work)
        self._thread.start()
        error = self._out_queue.get()
        if isinstance(error, Exception):
            raise error

    @property
    def db_url(self):
        return str(self._db_map.db_url)

    def shutdown(self):
        self._in_queue.put(self._CLOSE)
        self._thread.join()

    def _do_work(self):
        try:
            self._db_map = DatabaseMapping(
                self._db_url, upgrade=self._upgrade, memory=self._memory, create=self._create
            )
            self._out_queue.put(None)
        except Exception as error:  # pylint: disable=broad-except
            self._out_queue.put(error)
            return
        while True:
            input_ = self._in_queue.get()
            if input_ == self._CLOSE:
                self._db_map.connection.close()
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
            result = handler(*args, **kwargs)
            self._out_queue.put(result)

    def run(self, request, args, kwargs):
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
        return dict(result=result)

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
        return dict(result=result)

    def _do_import_data(self, data, comment):
        count, errors = import_data(self._db_map, unparse_value=_unparse_value, **data)
        if count and comment:
            try:
                self._db_map.commit_session(comment)
            except DBAPIError:
                self._db_map.rollback_session()
        return dict(result=(count, errors))

    def _do_export_data(self, **kwargs):
        return dict(result=export_data(self._db_map, parse_value=_parse_value, **kwargs))

    def _do_call_method(self, method_name, *args, **kwargs):
        method = getattr(self._db_map, method_name)
        result = method(*args, **kwargs)
        return dict(result=result)

    def _do_clear_filters(self):
        self._db_map.restore_entity_sq_maker()
        self._db_map.restore_entity_class_sq_maker()
        self._db_map.restore_parameter_definition_sq_maker()
        self._db_map.restore_parameter_value_sq_maker()
        self._db_map.restore_alternative_sq_maker()
        self._db_map.restore_scenario_sq_maker()
        self._db_map.restore_scenario_alternative_sq_maker()
        return dict(result=True)

    def _do_apply_filters(self, configs):
        try:
            apply_filter_stack(self._db_map, configs)
            return dict(result=True)
        except Exception as error:  # pylint: disable=broad-except
            return dict(error=str(error))


class _DBManager:
    def __init__(self):
        self._workers = {}

    def open_db_map(self, server_address, db_url, upgrade, memory):
        worker = self._workers.get(server_address)
        if worker is None:
            try:
                worker = self._workers[server_address] = _DBWorker(db_url, upgrade, memory)
            except Exception as error:  # pylint: disable=broad-except
                return dict(error=str(error))
        return dict(result=True)

    def close_db_map(self, server_address):
        worker = self._workers.pop(server_address, None)
        if worker is None:
            return dict(result=False)
        worker.shutdown()
        return dict(result=True)

    def get_db_url(self, server_address):
        worker = self._workers.get(server_address)
        if worker is not None:
            return worker.db_url

    def _run_request(self, server_address, request, args=(), kwargs=None, create=True):
        if kwargs is None:
            kwargs = {}
        worker = self._workers.get(server_address)
        if worker is not None:
            return worker.run(request, args, kwargs)

    def query(self, server_address, *args):
        return self._run_request(server_address, "query", args=args)

    def filtered_query(self, server_address, **kwargs):
        return self._run_request(server_address, "filtered_query", kwargs=kwargs)

    def import_data(self, server_address, data, comment):
        return self._run_request(server_address, "import_data", args=(data, comment))

    def export_data(self, server_address, **kwargs):
        return self._run_request(server_address, "export_data", kwargs=kwargs)

    def call_method(self, server_address, method_name, *args, **kwargs):
        return self._run_request(server_address, "call_method", args=(method_name, *args), kwargs=kwargs)

    def apply_filters(self, server_address, configs):
        return self._run_request(server_address, "apply_filters", args=(configs,))

    def clear_filters(self, server_address):
        return self._run_request(server_address, "clear_filters")


_db_manager = _DBManager()


class HandleDBMixin:
    def get_db_url(self):
        """
        Returns:
            str: The underlying db url
        """
        return _db_manager.get_db_url(self.server_address)

    def query(self, *args):
        """
        Runs queries.

        Returns:
            dict: where result is a dict from subquery name to a list of items from thay subquery, if successful.
        """
        return _db_manager.query(self.server_address, *args)

    def filtered_query(self, **kwargs):
        """
        Runs queries with filters.

        Returns:
            dict: where result is a dict from subquery name to a list of items from thay subquery, if successful.
        """
        return _db_manager.filtered_query(self.server_address, **kwargs)

    def import_data(self, data, comment):
        """Imports data and commit.

        Args:
            data (dict)
            comment (str)
        Returns:
            dict: where result is a list of import errors, if successful.
        """
        return _db_manager.import_data(self.server_address, data, comment)

    def export_data(self, **kwargs):
        """Exports data.

        Returns:
            dict: where result is the data exported from the db
        """
        return _db_manager.export_data(self.server_address, **kwargs)

    def call_method(self, method_name, *args, **kwargs):
        """Calls a method from the DatabaseMapping class.

        Args:
            method_name (str): the method name
            args: positional arguments passed to the method call
            kwargs: keyword arguments passed to the method call

        Returns:
            dict: where result is the return value of the method
        """
        return _db_manager.call_method(self.server_address, method_name, *args, **kwargs)

    def apply_filters(self, filters):
        configs = [
            {"scenario": scenario_filter_config, "tool": tool_filter_config, "alternatives": alternative_filter_config}[
                key
            ](value)
            for key, value in filters.items()
        ]
        return _db_manager.apply_filters(self.server_address, configs)

    def clear_filters(self):
        return _db_manager.clear_filters(self.server_address)

    def db_checkin(self):
        _ManagerRequestHandler(self.server_manager_queue).db_checkin(self.server_address)
        return dict(result=True)

    def db_checkout(self):
        _ManagerRequestHandler(self.server_manager_queue).db_checkout(self.server_address)
        return dict(result=True)

    def cancel_db_checkout(self):
        _ManagerRequestHandler(self.server_manager_queue).cancel_db_checkout(self.server_address)
        return dict(result=True)

    def open_db_map(self, db_url, upgrade, memory):
        return _db_manager.open_db_map(self.server_address, db_url, upgrade, memory)

    def close_db_map(self):
        return _db_manager.close_db_map(self.server_address)

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
        if client_version < _required_client_version:
            return dict(error=1, result=_required_client_version)
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
            "open_db_map": self.open_db_map,
            "close_db_map": self.close_db_map,
        }.get(request)
        if handler is None:
            return dict(error=f"invalid request '{request}'")
        try:
            return handler(*args, **kwargs)
        except Exception:  # pylint: disable=broad-except
            return dict(error=traceback.format_exc())

    def handle_request(self, request):
        response = self._get_response(request)
        return encode(response)


class DBHandler(HandleDBMixin):
    def __init__(self, db_url, upgrade=False, memory=False):
        self.server_address = uuid.uuid4().hex
        error = _db_manager.open_db_map(self.server_address, db_url, upgrade, memory).get("error")
        if error:
            raise RuntimeError(error)
        atexit.register(self.close)

    def close(self):
        _db_manager.close_db_map(self.server_address)


class DBRequestHandler(ReceiveAllMixing, HandleDBMixin, socketserver.BaseRequestHandler):
    """
    The request handler class for our server.
    """

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


def quick_db_checkout(server_manager_queue, ordering):
    _ManagerRequestHandler(server_manager_queue).quick_db_checkout(ordering)


def start_spine_db_server(server_manager_queue, db_url, upgrade=False, memory=False, ordering=None):
    """
    Args:
        db_url (str): Spine db url
        upgrade (bool): Whether to upgrade db or not
        memory (bool): Whether to use an in-memory database together with a persistent connection to it

    Returns:
        tuple: server address (e.g. (127.0.0.1, 54321))
    """
    handler = _ManagerRequestHandler(server_manager_queue)
    server_address = handler.start_server(db_url, upgrade, memory, ordering)
    return server_address


def shutdown_spine_db_server(server_manager_queue, server_address):
    handler = _ManagerRequestHandler(server_manager_queue)
    handler.shutdown_server(server_address)


@contextmanager
def closing_spine_db_server(server_manager_queue, db_url, upgrade=False, memory=False, ordering=None):
    server_address = start_spine_db_server(server_manager_queue, db_url, memory=memory, ordering=ordering)
    host, port = server_address
    try:
        yield urlunsplit(("http", f"{host}:{port}", "", "", ""))
    finally:
        shutdown_spine_db_server(server_manager_queue, server_address)


@contextmanager
def db_server_manager():
    mngr = _DBServerManager()
    try:
        yield mngr.queue
    finally:
        mngr.shutdown()
