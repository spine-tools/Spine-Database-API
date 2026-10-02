from spinedb_api.compat.encode import de_encode, re_encode

data = ["a", "b", "a", "a", None, "b"]

de_values = ["a", "b", None]
de_indices = [0, 1, 0, 0, 2, 1]

re_values = ["a", "b", "a", None, "b"]
re_run_end = [1, 2, 4, 5, 6]


def test_de_encode():
    arr_dict = de_encode("foo", data)
    assert arr_dict["values"] == de_values
    assert arr_dict["indices"] == de_indices  # type: ignore


def test_re_encode():
    arr_dict = re_encode("foo", data)
    assert arr_dict["values"] == re_values
    assert arr_dict["run_end"] == re_run_end  # type: ignore
