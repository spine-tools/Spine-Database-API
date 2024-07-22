"""
This benchmark tests the performance of reading a Map type value from database.
"""

import time
import pyperf
from benchmarks.utils import build_even_map
from spinedb_api import from_database, to_database


def value_from_database(loops, db_value, value_type):
    duration = 0.0
    for _ in range(loops):
        start = time.perf_counter()
        from_database(db_value, value_type)
        duration += time.perf_counter() - start
    return duration


def run_benchmark(file_name):
    runner = pyperf.Runner(loops=3)
    runs = {
        "value_from_database[Map(10, 10, 100)]": {"dimensions": (10, 10, 100)},
        "value_from_database[Map(1000)]": {"dimensions": (10000,)},
    }
    for name, parameters in runs.items():
        db_value, value_type = to_database(build_even_map(parameters["dimensions"]))
        benchmark = runner.bench_time_func(
            name,
            value_from_database,
            db_value,
            value_type,
        )
        if file_name and benchmark is not None:
            pyperf.add_runs(file_name, benchmark)


if __name__ == "__main__":
    run_benchmark("")
