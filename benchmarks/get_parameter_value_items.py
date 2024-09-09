"""
This benchmark tests the performance of getting a list of parameter value items from database mapping.
"""

import pathlib
from tempfile import TemporaryDirectory
import time
import pyperf
from spinedb_api import DatabaseMapping, to_database
from spinedb_api.mapped_items import EntityItem, ParameterDefinitionItem


def get_value_items(
    loops: int, db_map: DatabaseMapping, entity_item: EntityItem, parameter_definition_item: ParameterDefinitionItem
) -> float:
    duration = 0.0
    for _ in range(loops):
        start = time.perf_counter()
        values = db_map.get_parameter_value_items(
            entity_class_name=entity_item["entity_class_name"],
            entity_byname=entity_item["entity_byname"],
            parameter_definition_name=parameter_definition_item["name"],
        )
        duration += time.perf_counter() - start
        assert len(values) == 1
    return duration


def run_benchmark(file_name: str) -> None:
    runner = pyperf.Runner()
    with TemporaryDirectory() as temp_dir:
        db_path = pathlib.Path(temp_dir) / "db.sqlite"
        url = "sqlite:///" + str(db_path)
        with DatabaseMapping(url, create=True) as db_map:
            db_map.add_entity_class_item(name="Widget")
            db_map.add_entity_item(name="spoon", entity_class_name="Widget")
            db_map.add_parameter_definition_item(name="weight", entity_class_name="Widget")
            value, value_type = to_database(2.3)
            db_map.add_parameter_value_item(
                entity_class_name="Widget",
                entity_byname=("spoon",),
                parameter_definition_name="weight",
                alternative_name="Base",
                value=value,
                type=value_type,
            )
            db_map.commit_session("Add data")
        with DatabaseMapping(url) as db_map:
            entity_item = db_map.get_entity_item(name="spoon", entity_class_name="Widget")
            parameter_definition_item = db_map.get_parameter_definition_item(name="weight", entity_class_name="Widget")
            benchmark = runner.bench_time_func(
                "get value items", get_value_items, db_map, entity_item, parameter_definition_item
            )
        if file_name and benchmark is not None:
            pyperf.add_runs(file_name, benchmark)


if __name__ == "__main__":
    run_benchmark("")
