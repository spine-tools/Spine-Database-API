"""
This benchmark tests the performance of the MappedItemBase.__getitem__() method.
"""

import time
from typing import Dict
import pyperf
from spinedb_api import DatabaseMapping
from spinedb_api.db_mapping_base import PublicItem


def use_subscript_operator(loops: int, items: PublicItem, field: Dict):
    duration = 0.0
    for _ in range(loops):
        for item in items:
            start = time.perf_counter()
            value = item[field]
            duration += time.perf_counter() - start
    return duration


def run_benchmark(file_name):
    runner = pyperf.Runner()
    inner_loops = 1000
    object_class_names = [str(i) for i in range(inner_loops)]
    relationship_class_names = [f"r{dimension}" for dimension in object_class_names]
    with DatabaseMapping("sqlite://", create=True) as db_map:
        object_classes = []
        for name in object_class_names:
            item, error = db_map.add_entity_class_item(name=name)
            assert error is None
            object_classes.append(item)
        relationship_classes = []
        for name, dimension in zip(relationship_class_names, object_classes):
            item, error = db_map.add_entity_class_item(name, dimension_name_list=(dimension["name"],))
            assert error is None
            relationship_classes.append(item)
    benchmarks = [
        runner.bench_time_func(
            "PublicItem subscript['name' in EntityClassItem]",
            use_subscript_operator,
            object_classes,
            "name",
            inner_loops=inner_loops,
        ),
        runner.bench_time_func(
            "PublicItem subscript['dimension_name_list' in EntityClassItem]",
            use_subscript_operator,
            relationship_classes,
            "dimension_name_list",
            inner_loops=inner_loops,
        ),
    ]
    if file_name:
        for benchmark in benchmarks:
            if benchmark is not None:
                pyperf.add_runs(file_name, benchmark)


if __name__ == "__main__":
    run_benchmark("")
