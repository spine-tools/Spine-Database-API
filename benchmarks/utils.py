import datetime
import math
from spinedb_api import __version__, DateTime, Map


def build_sizeable_map():
    start = datetime.datetime(year=2024, month=1, day=1)
    root_xs = []
    root_ys = []
    i_max = 10
    j_max = 10
    k_max = 10
    total = i_max * j_max * k_max
    for i in range(i_max):
        root_xs.append(DateTime(start + datetime.timedelta(hours=i)))
        leaf_xs = []
        leaf_ys = []
        for j in range(j_max):
            leaf_xs.append(DateTime(start + datetime.timedelta(hours=j)))
            xs = []
            ys = []
            for k in range(k_max):
                xs.append(DateTime(start + datetime.timedelta(hours=k)))
                x = float(k + k_max * j + j_max * i) / total
                ys.append(math.sin(x * math.pi / 2.0) + (x * j) ** 2 + x * i)
            leaf_ys.append(Map(xs, ys))
        root_ys.append(Map(leaf_xs, leaf_ys))
    return Map(root_xs, root_ys)


def run_file_name():
    return f"benchmark-{__version__}.json"
