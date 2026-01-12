# Pato

**Pato** is a lightweight, in-memory SQL database server built on **DuckDB** that allows you to query, summarize, and transform data files interactively via a simple command-line interface. It supports CSV, Parquet, and JSON/JSONL file formats and provides quick data inspection, aggregation, and export capabilities.

---

## Features

* Load CSV, Parquet, JSON, and JSONL files into memory as tables.
* Inspect tables: view columns, data types, summary statistics, first/last rows, and row counts.
* Execute arbitrary SQL queries on in-memory data.
* Perform common aggregation operations (`SUM`, `AVG`, `MIN`, `MAX`) on numeric columns.
* Export tables to CSV, Parquet, or JSON formats.
* Lightweight Unix socket server for multi-process access.
* Server can be run in the background while sending commands from other processes.

---

## Installation

Pato can be installed either from PyPI or directly from source.

---

### Option 1: Install from PyPI (recommended)

If you just want to use Pato:

```bash
pip install pato
```

Verify installation:
```bash
pato --help
```

This installs the `pato` command and all required dependencies.

### Option 2: Install from source using `uv`
```bash
git clone <repo-url>
cd pato
```
Install dependencies and the `pato` CLI using uv:
```bash
uv sync
```
This will:
- Create an isolated environment
- Install all dependencies
- Register the `pato` command

Verify:
```bash
pato --help
```

---

## Usage

### Start the server

Start the Pato server in a terminal:

```bash
pato run
```

This will start a server listening on the default Unix socket:

```
~/.pato/pato.sock
```

The server **must be running** before executing any other commands from another process.

---

### Sending commands

In another terminal, you can send commands to the running server using:

```bash
pato <command> [arguments]
```

For example:

```bash
pato load example.csv
pato head example --n 5
```

> The `--socket` argument can be used to specify a custom socket path if desired.

---

## Commands

### Server & utility

| Command   | Description                            |
| --------- | -------------------------------------- |
| `run`     | Start the Pato server                  |
| `stop`    | Shutdown the running server            |
| `ping`    | Check if the server is alive           |
| `version` | Show Pato, DuckDB, and Python versions |

### Data loading & exporting

| Command                                     | Description                                                                        |
| ------------------------------------------- | ---------------------------------------------------------------------------------- |
| `load file [--name NAME] [--format FORMAT]` | Load CSV, Parquet, or JSON/JSONL into memory. Default table name is the file name. |
| `export table file`                         | Export a table to CSV, Parquet, or JSON.                                           |

### Table inspection

| Command              | Description                                 |
| -------------------- | ------------------------------------------- |
| `list`               | List all tables currently loaded            |
| `describe table`     | Show table columns and data types           |
| `summarize table`    | Show summary statistics for numeric columns |
| `head table [--n N]` | Show first N rows of a table (default 10)   |
| `tail table [--n N]` | Show last N rows of a table (default 10)    |
| `count table`        | Return the number of rows in a table        |
| `columns table`      | (Optional future) List only column names    |
| `types table`        | (Optional future) List only column types    |

### Data transformation

| Command                    | Description                                     |
| -------------------------- | ----------------------------------------------- |
| `exec --sql="..."`           | Execute arbitrary SQL query on in-memory tables. If sql is left off then you will will be prompted with multi line support. Enter a blank line to execute. |
| `drop table`               | Drop a table from memory                        |
| `rename old_name new_name` | Rename a table in memory                        |

### Aggregation commands

| Command            | Description             |
| ------------------ | ----------------------- |
| `sum column table` | Compute SUM of a column |
| `avg column table` | Compute AVG of a column |
| `min column table` | Compute MIN of a column |
| `max column table` | Compute MAX of a column |

---

## Notes

* The server **must be running** in a separate process for commands to execute.
* Only **Unix sockets** are currently supported (Linux, macOS, WSL). Windows native is not supported.
* File paths passed to `load` and `export` can be relative or absolute. Relative paths are resolved relative to the client working directory.
* Export directories will be automatically created if they do not exist.


---

## Example Workflow

1. Start server:

```bash
pato run
```

2. Load a CSV file:

```bash
pato load data/sales.csv
```

3. Inspect data:

```bash
pato list
pato describe sales
pato head sales --n 5
pato count sales
```

4. Perform aggregations:

```bash
pato sum revenue sales
pato avg price sales
```

5. Export data:

```bash
pato export sales out/sales.parquet
```

6. Stop server:

```bash
pato stop
```

---

## Pato Shell

The `pato shell` command allows you to run enter a SQL shell to run queries without `pato exec`. Any `pato <command>` will still work to conveniently load, export, summarize, etc.

---

## License

MIT License

---
## Future Features
* Integration with visualization tools for quick plotting.
* Loading and exporting the **Duck DB** instance to a file for persistance
