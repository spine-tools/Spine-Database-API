""" This benchmark tests the performance of reading a DateTime value from database. """

import datetime
import time
from typing import Any, Sequence, Tuple
import pyperf
from spinedb_api import DateTime, from_database, to_database


def build_datetimes(count: int) -> Sequence[DateTime]:
    datetimes = []
    year = 2024
    month = 1
    day = 1
    hour = 0
    while len(datetimes) != count:
        datetimes.append(DateTime(datetime.datetime(year, month, day, hour)))
        hour += 1
        if hour == 24:
            hour = 0
            day += 1
            if day == 29:
                day = 1
                month += 1
                if month == 13:
                    month = 1
                    year += 1
    return datetimes


def value_from_database(loops: int, db_values_and_types: Sequence[Tuple[Any, str]]) -> float:
    duration = 0.0
    for _ in range(loops):
        for db_value, db_type in db_values_and_types:
            start = time.perf_counter()
            from_database(db_value, db_type)
            duration += time.perf_counter() - start
    return duration


def run_benchmark(file_name):
    runner = pyperf.Runner(loops=10)
    inner_loops = 1000
    db_values_and_types = [to_database(x) for x in build_datetimes(inner_loops)]
    benchmark = runner.bench_time_func(
        "from_database[DateTime]", value_from_database, db_values_and_types, inner_loops=inner_loops
    )
    if file_name:
        pyperf.add_runs(file_name, benchmark)


if __name__ == "__main__":
    run_benchmark("")
