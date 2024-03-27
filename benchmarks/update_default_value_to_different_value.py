"""
This benchmark tests the performance of updating a parameter definition when
the update changes the default value from None to a somewhat complex Map.
"""

import time
import pyperf
from spinedb_api import DatabaseMapping, to_database
from benchmarks.utils import build_sizeable_map, run_file_name


def update_default_value(loops, db_map, first_db_value, first_value_type, second_db_value, second_value_type):
    total_time = 0.0
    for counter in range(loops):
        start = time.perf_counter()
        result = db_map.update_parameter_definition_item(
            name="x", entity_class_name="Object", default_value=second_db_value, default_type=second_value_type
        )
        finish = time.perf_counter()
        error = result[1]
        if error:
            raise RuntimeError(error)
        total_time += finish - start
        db_map.update_parameter_definition_item(
            name="x", entity_class_name="Object", default_value=first_db_value, default_type=first_value_type
        )
    return total_time


def run_benchmark():
    first_value, first_type = to_database(None)
    second_value, second_type = to_database(build_sizeable_map())
    with DatabaseMapping("sqlite://", create=True) as db_map:
        db_map.add_entity_class_item(name="Object")
        db_map.add_parameter_definition_item(
            name="x", entity_class_name="Object", default_value=first_value, default_type=first_type
        )
        runner = pyperf.Runner(min_time=0.0001)
        benchmark = runner.bench_time_func(
            "update_parameter_definition_item[None,Map]",
            update_default_value,
            db_map,
            first_value,
            first_type,
            second_value,
            second_type,
            inner_loops=10,
        )
    pyperf.add_runs(run_file_name(), benchmark)


if __name__ == "__main__":
    run_benchmark()
