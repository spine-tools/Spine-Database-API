"""
This benchmark tests the performance of from_database_to_dimension_count() for nested maps.
"""

import time
import pyperf
from spinedb_api import Map, to_database
from spinedb_api.parameter_value import from_database_to_dimension_count

MAP_DEPTH = 3
MAP_LENGTH = 24


def build_map() -> bytes:
    xs = list(map(float, range(MAP_LENGTH)))
    ys = list(xs)
    depth = MAP_DEPTH
    while True:
        depth -= 1
        if depth == 0:
            return to_database(Map(xs, ys))[0]
        else:
            ys = [Map(xs, ys) for _ in range(MAP_LENGTH)]


def dimension_count(loops: int, unparsed_map: bytes) -> float:
    duration = 0.0
    map_type = Map.TYPE
    for _ in range(loops):
        start = time.perf_counter()
        rank = from_database_to_dimension_count(unparsed_map, map_type)
        duration += time.perf_counter() - start
        assert rank == MAP_DEPTH
    return duration


def run_benchmark(file_name: str) -> None:
    runner = pyperf.Runner()
    unparsed_map = build_map()
    benchmark = runner.bench_time_func("from_database[DateTime]", dimension_count, unparsed_map)
    if file_name:
        pyperf.add_runs(file_name, benchmark)


if __name__ == "__main__":
    run_benchmark("")
