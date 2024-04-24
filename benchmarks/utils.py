import datetime
import math
from typing import Sequence
from spinedb_api import DateTime, Map


def build_map(size: int) -> Map:
    start = datetime.datetime(year=2024, month=1, day=1)
    xs = []
    ys = []
    for i in range(size):
        xs.append(DateTime(start + datetime.timedelta(hours=i)))
        x = i / size
        ys.append(math.sin(x * math.pi / 2.0) + x)
    return Map(xs, ys)


def build_even_map(shape: Sequence[int] = (10, 10, 10)) -> Map:
    if not shape:
        return Map([], [], index_type=DateTime)
    if len(shape) == 1:
        return build_map(shape[0])
    xs = []
    ys = []
    for i in range(shape[0]):
        start = datetime.datetime(year=2024, month=1, day=1)
        xs.append(DateTime(start + datetime.timedelta(hours=i)))
        ys.append(build_even_map(shape[1:]))
    return Map(xs, ys)
