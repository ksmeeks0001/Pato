import json
import os
import socket
import socketserver
import threading 
import argparse
import inspect
import cmd
import shlex

import duckdb

VERSION = "0.1.0"

SOCKET_PATH = os.path.expanduser("~/.pato/pato.sock")
os.makedirs(os.path.dirname(SOCKET_PATH), exist_ok=True)

# command registry
COMMANDS = {}

parser = argparse.ArgumentParser()
parser.add_argument(
    "--socket",
    default=SOCKET_PATH,
)

subparsers = parser.add_subparsers(dest="command", required=True)
subparsers.add_parser("run", help="Start memory database")
subparsers.add_parser("shell", help="Start sql shell to Duck DB instance")

def command(name=None, arg_help={}):
    """Decorator that marks a method as a command and builds script arguments."""
    
    def decorator(func):
        cmd_name = name or func.__name__
        COMMANDS[cmd_name] = func  # store original function (not bound yet)
        command_parser = subparsers.add_parser(cmd_name, help=func.__doc__)
        sig = inspect.signature(func)
        for cmd, param in sig.parameters.items():
            if cmd == "self":
                continue
            required = param.default != inspect._empty
            command_parser.add_argument(cmd if not required else f"--{cmd}", help=arg_help.get(cmd, None))
        return func
    return decorator

# register data aggregation commands dynamically
AGG_FUNCS = ["sum", "avg", "min", "max"]

for agg in AGG_FUNCS:
    def make_agg_cmd(func_name):
        @command(func_name, {"column": "column name", "table": "table name"})
        def agg_command(self, column, table):
            tables = {t[0] for t in self.db.execute("SHOW TABLES").fetchall()}
            if table not in tables:
                return f"Table not found: {table}"

            cols = [c[0] for c in self.db.execute(f"DESCRIBE {table}").fetchall()]
            if column not in cols:
                return f"Column '{column}' not found in table '{table}'"

            try:
                result = self.db.execute(f"SELECT {func_name.upper()}({column}) FROM {table}").fetchone()[0]
                return f"{func_name.upper()}({column}) = {result}"
            except Exception as e:
                return f"Error computing {func_name}: {e}"
        return agg_command

    make_agg_cmd(agg)


def input_multi(msg):
    
    results = []
    tmp = input(msg).strip()
    while tmp != "":
        results.append(tmp)
        tmp = input().strip()

    return "\n".join(results)


def send(socket_path, cmd, **kwargs):
    """Send a command to the running Pato server"""

    if not os.path.exists(socket_path):
        return "Pato is not running (socket not found). Start it with: pato run"

    # normalize file paths for known args
    if "file" in kwargs:
        kwargs["file"] = os.path.abspath(kwargs["file"])

    if cmd == "exec" and kwargs["sql"] is None:
        kwargs["sql"] = input_multi("-- ENTER SQL\n")

    payload = {"cmd": cmd, **kwargs}

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(socket_path)
            client.sendall((json.dumps(payload) + "\n").encode())
            return client.recv(1024 * 1024).decode().strip()

    except ConnectionRefusedError:
        return "Pato is not running (connection refused)."

    except OSError as e:
        if e.errno == errno.ENOENT:
            return "Pato is not running (socket disappeared)."
        raise


class PatoShell(cmd.Cmd):

    intro = "Entering Pato SQL Shell. End SQL Statements with ;"
    prompt = "Pato> "
    cont_prompt = "....>"

    def __init__(self, parser, socket=SOCKET_PATH):
        super().__init__()
        self.buffer = []
        self.socket = socket
        self.parser = parser

    def default(self, line):
        """Default input is sent to Duck DB"""
        self.buffer.append(line)

        if not self._statement_complete(line):
            self.prompt = self.cont_prompt
            return

        sql = "\n".join(self.buffer).strip()
        self.buffer.clear()
        self.prompt = "pato> "

        if sql:
            result = send(self.socket, "exec", sql=sql)
            print(result)

    def _statement_complete(self, line):
        sql = "\n".join(self.buffer)
        in_string = False
        for c in sql:
            if c == "'" and not in_string:
                in_string = True
            elif c == "'" and in_string:
                in_string = False
        return not in_string and sql.rstrip().endswith(";")

    def do_pato(self, line):
        try:
            argv = shlex.split(line)
            args = self.parser.parse_args(argv)
            command_args = get_command_args(args)
        except ValueError as e:        # shlex error
            print(f"Parse error: {e}")
        except SystemExit:             # argparse error
            return

        result = send(self.socket, args.command, **command_args)
        print(result)
  
    def do_exit(self, _):
        return True

    def do_quit(self, _):
        return True

    def do_EOF(self, _):
        print()
        


class Pato:
    def __init__(self, socket_path=SOCKET_PATH):
        self.db = duckdb.connect(":memory:")
        self.socket_path = socket_path
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        # bind registered functions to this instance dynamically
        self.commands = {
            name: func.__get__(self, self.__class__)
            for name, func in COMMANDS.items()
        }
        self.server = None

    # ---------------- COMMAND EXECUTION ---------------- #

    def execute_command(self, cmd, **kwargs):
        if cmd not in self.commands:
            return f"Unknown command '{cmd}'"
        return self.commands[cmd](**kwargs)    

    # ---------- Socket Server ----------
    def serve(self):
        pato_instance = self  # capture for RequestHandler class below

        class Handler(socketserver.StreamRequestHandler):
            def handle(self_inner):
                try:
                    data = json.loads(self_inner.rfile.readline().decode())
                    cmd = data.pop("cmd")
                    result = pato_instance.execute_command(cmd, **data)
                except Exception as e:
                    result = f"Error: {e}"

                self_inner.wfile.write((str(result) + "\n").encode())

        self.server = socketserver.UnixStreamServer(self.socket_path, Handler)
        print(f"Pato listening on {self.socket_path}")
        self.server.serve_forever()
        
    # ---------------- SERVER COMMANDS ---------------- #
    @command()
    def ping(self):
        """Check if server is alive"""
        return "pong"

    @command()
    def stop(self):
        """Shutdown memory database"""
        def _shutdown():
            self.server.shutdown()
            self.server.server_close()
            self.cleanup_socket()
            print("Pato stopped.")

        threading.Thread(target=_shutdown).start()
        return "Pato stopped"

    def cleanup_socket(self):
        
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

    @command()
    def version(self):
        """Show Pato and DuckDB version"""
        import platform
        pato_version = VERSION  # Update if you release a new version
        duck_version = duckdb.__version__
        python_version = platform.python_version()
        return f"Pato {pato_version} | DuckDB {duck_version} | Python {python_version}"


    # ---------------- DATABASE COMMANDS ---------------- #

    @command(
        arg_help ={
            "file": "file to load (csv, parquet, json, jsonl)",
            "name": "optional table name (default: file name)",
            "format": "Optional explicit file type"
        },
    )
    def load(self, file, name=None, format=None):
        """Load file into memory as a table"""

        READERS = {
            ".csv": "read_csv_auto",
            ".parquet": "read_parquet",
            ".pq": "read_parquet",
            ".json": "read_json_auto",
            ".jsonl": "read_json",
        }

        if not os.path.exists(file):
            return f"File not found: {file}"

        ext = format or os.path.splitext(file)[1].lower()

        if ext not in READERS:
            return f"Unsupported file type: {ext}"

        if not name:
            name = os.path.splitext(os.path.basename(file))[0]

        reader = READERS[ext]

        sql = f"""
            CREATE TABLE {name} AS
            SELECT * FROM {reader}(?)
        """

        self.db.execute(sql, [file])
        return f"Loaded '{file}' as '{name}'"

    @command(
        arg_help = {
            "table": "table name to export",
            "file": "output file path (csv, parquet, json)",
        },
    )
    def export(self, table, file):
        """Export table to a file"""

        EXPORT_FORMATS = {
            ".csv": "CSV",
            ".parquet": "PARQUET",
            ".pq": "PARQUET",
            ".json": "JSON",
        }

        tables = {
            t[0] for t in self.db.execute("SHOW TABLES").fetchall()
        }

        if table not in tables:
            return f"Table not found: {table}"

        ext = os.path.splitext(file)[1].lower()

        if ext not in EXPORT_FORMATS:
            return f"Unsupported export format: {ext}"

        fmt = EXPORT_FORMATS[ext]

        os.makedirs(os.path.dirname(file) or ".", exist_ok=True)

        sql = f"""
            COPY {table}
            TO ?
            (FORMAT {fmt}, HEADER TRUE)
        """

        self.db.execute(sql, [file])
        return f"Exported '{table}' to '{file}'"

    @command()
    def describe(self, table):
        """Describe table columns and data types"""
        try:
            df = self.db.execute(f"DESCRIBE {table}").fetchdf()
            return df.to_string(index=False)
        except Exception as e:
            return f"Error describing table '{table}': {e}"

    @command()
    def summarize(self, table):
        """Show summary statistics for a table"""
        try:
            df = self.db.execute(f"SUMMARIZE {table}").fetchdf()
            return df.to_string(index=False)
        except Exception as e:
            return f"Error summarizing table '{table}': {e}"

    @command(arg_help = {"table": "table name", "n": "number of rows to show, default 10"})
    def head(self, table, n=10):
        """Show first N rows of a table"""
        tables = {t[0] for t in self.db.execute("SHOW TABLES").fetchall()}
        if table not in tables:
            return f"Table not found: {table}"
        try:
            df = self.db.execute(f"SELECT * FROM {table} LIMIT {n}").fetchdf()
            return df.to_string(index=False)
        except Exception as e:
            return f"Error fetching head: {e}"

    @command(arg_help = {"table": "table name", "n": "number of rows to show, default 10"})
    def tail(self, table, n=10):
        """Show last N rows of a table"""
        tables = {t[0] for t in self.db.execute("SHOW TABLES").fetchall()}
        if table not in tables:
            return f"Table not found: {table}"
        try:
            total = self.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            offset = max(total - n, 0)
            df = self.db.execute(f"SELECT * FROM {table} OFFSET {offset} LIMIT {n}").fetchdf()
            return df.to_string(index=False)
        except Exception as e:
            return f"Error fetching tail: {e}"

    @command(arg_help = {"table": "table name"})
    def count(self, table):
        """Return number of rows in a table"""
        tables = {t[0] for t in self.db.execute("SHOW TABLES").fetchall()}
        if table not in tables:
            return f"Table not found: {table}"
        try:
            rows = self.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            return f"{table} has {rows} rows"
        except Exception as e:
            return f"Error counting rows: {e}"

    @command("list")
    def list_tables(self):
        """List tables currently loaded"""
        rows = self.db.execute("SHOW TABLES").fetchall()
        return "\n".join(t[0] for t in rows) or "(no tables)"

    @command(arg_help = {"old_name": "current table name", "new_name": "new table name"})
    def rename(self, old_name, new_name):
        """Rename a table"""
        tables = {t[0] for t in self.db.execute("SHOW TABLES").fetchall()}
        if old_name not in tables:
            return f"Table not found: {old_name}"
        if new_name in tables:
            return f"Cannot rename: {new_name} already exists"
        try:
            self.db.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
            return f"Renamed '{old_name}' to '{new_name}'"
        except Exception as e:
            return f"Error renaming table: {e}"


    @command(arg_help={"sql": "SQL to execute. If ommited then multi line input prompt"})
    def exec(self, sql=None):
        """Run SQL query"""
        if sql is None:
            return "No SQL Provided"
        
        try:
            return self.db.execute(sql).fetchdf().to_string()
        except Exception as e:
            return str(e)

    @command()
    def drop(self, table):
        """Drop table"""
        self.db.execute(f"DROP TABLE {table}")
        return f"Dropped '{table}'"
  

def get_command_args(args):
    command_args = dict(vars(args))
    del command_args["command"]
    if command_args.get("socket", None) is not None:
        del command_args["socket"]
    return command_args

def main():
    args = parser.parse_args()
    
    if args.command == "run":
        pato = Pato(args.socket)
        pato.serve()
    elif args.command == "shell":
        shell = PatoShell(parser, args.socket)
        shell.cmdloop()
        print("Pato Shell Terminated")
    else:
        command_args = get_command_args(args)
        result = send(args.socket, args.command, **command_args)
        print(result)

    
if __name__ == '__main__':
    main()