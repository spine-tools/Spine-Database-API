"""
This benchmark tests the performance of parameter_value.type_for_scalar().
"""

import time
from typing import Iterable
import pyperf
from spinedb_api.parameter_value import type_for_scalar


def benchmark_type_for_scalar(loops: int, values: Iterable) -> float:
    duration = 0.0
    for _ in range(loops):
        for value in values:
            start = time.perf_counter()
            type_for_scalar(value)
            finish = time.perf_counter()
            duration += finish - start
    return duration


def run_benchmark(file_name: str) -> None:
    values = (2.3, False, "a string")
    label = "[" + ",".join(type(value).__name__ for value in values) + "]"
    runner = pyperf.Runner()
    benchmark = runner.bench_time_func(
        "type_for_scalar" + label,
        benchmark_type_for_scalar,
        values,
        inner_loops=len(values),
    )
    if file_name:
        pyperf.add_runs(file_name, benchmark)


if __name__ == "__main__":
    run_benchmark("")
