"""
This benchmark tests the performance of updating a parameter definition item when
the default value is somewhat complex Map and the update does not change anything.
"""
import time
import pyperf
from spinedb_api import DatabaseMapping, to_database
from benchmarks.utils import build_sizeable_map, run_file_name


def update_default_value(loops, db_map, value, value_type):
    total_time = 0.0
    for counter in range(loops):
        start = time.perf_counter()
        result = db_map.update_parameter_definition_item(
            name="x", entity_class_name="Object", default_value=value, default_type=value_type
        )
        finish = time.perf_counter()
        error = result[1]
        if error:
            raise RuntimeError(error)
        total_time += finish - start
    return total_time


def run_benchmark():
    value, value_type = to_database(build_sizeable_map())
    with DatabaseMapping("sqlite://", create=True) as db_map:
        db_map.add_entity_class_item(name="Object")
        db_map.add_parameter_definition_item(
            name="x", entity_class_name="Object", default_value=value, default_type=value_type
        )
        runner = pyperf.Runner()
        benchmark = runner.bench_time_func(
            "update_parameter_definition_item[Map,Map]", update_default_value, db_map, value, value_type, inner_loops=10
        )
    pyperf.add_runs(run_file_name(), benchmark)


if __name__ == "__main__":
    run_benchmark()
