######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
import multiprocessing
import pytest
from spinedb_api import DatabaseMapping, create_new_spine_database
from spinedb_api.spine_db_client import lock_db
from spinedb_api.spine_db_server import closing_spine_db_server, db_server_manager


@pytest.fixture
def db_url(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    create_new_spine_database(url)
    return url


def _do_work(url):
    with DatabaseMapping(url) as db_map:
        with lock_db(db_map) as lock:
            assert lock is None
            alternatives = db_map.find_alternatives()
            if len(alternatives) == 1:
                db_map.add_alternative(name="visited")
                db_map.commit_session("Added first alternative.")
            else:
                db_map.add_alternative(name="visited again")
                db_map.commit_session("Added second alternative.")


class TestLockDB:
    def test_locking_is_no_operation_when_no_server_is_used(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            with lock_db(db_map) as lock:
                assert lock is None

    def test_locking_with_server(self, db_url):
        with db_server_manager() as manager_queue:
            with (
                closing_spine_db_server(db_url, server_manager_queue=manager_queue) as server_url1,
                closing_spine_db_server(db_url, server_manager_queue=manager_queue) as server_url2,
            ):
                task1 = multiprocessing.Process(target=_do_work, args=(server_url1,))
                task2 = multiprocessing.Process(target=_do_work, args=(server_url2,))
                task1.start()
                task2.start()
                task1.join()
                task2.join()
        with DatabaseMapping(db_url) as db_map:
            alternatives = db_map.find_alternatives()
            assert {alt["name"] for alt in alternatives} == {"Base", "visited", "visited again"}
