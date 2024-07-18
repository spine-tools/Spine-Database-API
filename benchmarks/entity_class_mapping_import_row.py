"""
This benchmark tests the performance of import-mapping multidimensional entity classes.
"""

import time
from typing import Callable, Dict, List
import pyperf
from spinedb_api import get_mapped_data
from spinedb_api.import_mapping.type_conversion import value_to_convert_spec


def import_entity_classes(
    loops: int,
    mappings: List[List[Dict]],
    convert_functions: Dict,
    input_table: List[List[str]],
) -> float:
    duration = 0.0
    for _ in range(loops):
        data_source = iter(input_table)
        start = time.perf_counter()
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        duration += time.perf_counter() - start
        assert errors == []
        assert mapped_data
    return duration


def make_input_table(
    size: int, name_1: Callable[[int], str], name_2: Callable[[int], str], name_3: Callable[[int], str]
):
    table: List[List[str]] = []
    for i in range(size):
        dim_1 = name_1(i)
        dim_2 = name_2(i)
        dim_3 = name_3(i)
        class_name = f"{dim_1}__{dim_2}__{dim_3}"
        table.append([class_name, dim_1, dim_2, dim_3])
    return table


def run_benchmark(file_name: str) -> None:
    table_size = 100
    input_tables = [
        make_input_table(table_size, str, str, str),
        make_input_table(table_size, str, lambda i: str(i + 1), lambda i: str(i + 1)),
        make_input_table(table_size, str, lambda i: str(i + 1), lambda i: str(i + 2)),
    ]
    source_names = ["A__A__A", "A__B__B", "A__B__C"]
    mappings = [
        [
            {"map_type": "EntityClass", "position": 0},
            {"map_type": "Dimension", "position": 1},
            {"map_type": "Dimension", "position": 2},
            {"map_type": "Dimension", "position": 3},
        ]
    ]
    convert_function_specs = {0: "string", 1: "string", 2: "string", 3: "string"}
    convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
    runner = pyperf.Runner()
    for input_table, name in zip(input_tables, source_names):
        benchmark = runner.bench_time_func(
            f"import nD entity class[{name}]", import_entity_classes, mappings, convert_functions, input_table
        )
        if file_name and benchmark is not None:
            pyperf.add_runs(file_name, benchmark)


if __name__ == "__main__":
    run_benchmark("")
