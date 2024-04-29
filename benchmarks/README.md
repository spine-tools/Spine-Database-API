# Performance benchmarks

This Python package contains performance benchmarks for `spinedb_api`.
The benchmarks use [`pyperf`](https://pyperf.readthedocs.io/en/latest/index.html)
which is installed as part of the optional developer dependencies:

```commandline
python -mpip install -e .[dev]
```

Each Python file is a self-contained script
that benchmarks some aspect of the DB API. 
Benchmark results can be optionally written into a`.json` file
by modifying the script.
This may be handy for comparing different branches/commits/changes etc.
The file can be inspected by

```commandline
python -mpyperf show <benchmark file.json>
```

Benchmark files from e.g. different commits/branches can be compared by

```commandline
python -mpyperf compare_to <benchmark file 1.json> <benchmark file 2.json>
```

Check the [`pyperf` documentation](https://pyperf.readthedocs.io/en/latest/index.html)
for further things you can do with it.
