# Performance benchmarks

This Python package contains performance benchmarks for `spinedb_api`.
The benchmarks use [`pyperf`](https://pyperf.readthedocs.io/en/latest/index.html)
which can be installed by installing the optional developer dependencies:

```commandline
python -mpip install .[dev]
```

Each Python file is an individual script
that writes the run results into a common `.json` file.
The file can be inspected by

```commandline
python -mpyperf show <benchmark file.json>
```

Benchmarks from e.g. different commits/branches can be compared by

```commandline
python -mpyperf compare_to <benchmark file 1.json> <benchmark file 2.json>
```

Check the [`pyperf` documentation]((https://pyperf.readthedocs.io/en/latest/index.html))
for further things you can do with it.
