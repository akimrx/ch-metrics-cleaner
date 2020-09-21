#!/usr/bin/env python3

import os
import sys
import yaml
import time
import argparse
import requests
import configparser

from datetime import datetime

__version__ = "1.0.0"

# Arguments parse block
parser = argparse.ArgumentParser(
    prog="clickhouse-cleaner",
    description="ClickHouse data cleaner",
    formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=90)
)
commands = parser.add_argument_group("Arguments")
commands.add_argument("-p", "--prefix", metavar="str [, ...]", default="", type=str, help="Prefixes for search matches")
commands.add_argument("-k", "--key", metavar="str", type=str, help="Primary key in the table for search matches by prefix")
commands.add_argument("-d", "--database", metavar="str", type=str, help="Database to connect")
commands.add_argument("-t", "--table",  metavar="str [, ...]", type=str, required=True, help="Tables for search")
commands.add_argument("-S", "--checkout-only", action="store_true", help="Print only mutation status for table")
commands.add_argument("-W", "--await-mutation-end", action="store_true", help="Lock script execution until the mutation completes")
commands.add_argument("-f", "--force", action="store_true", help="Delete all matches without asking for confirmation (pretty output)")
commands.add_argument("-c", "--config", metavar="file", type=str, required=False, help="Custom path to config file in yaml format")
commands.add_argument("--version", action="version", version=__version__)
args = parser.parse_args()


# Configuration block
homedir = os.path.expanduser("~")
config_dir = os.path.join(homedir, ".config")
config_file = args.config or os.path.join(config_dir, "ch_cleaner.yaml")

try:
    with open(config_file, "r") as cfgfile:
        config = yaml.load(cfgfile, Loader=yaml.Loader)
        ENDPOINT = f"http://{config['clickhouse']['fqdn']}:{config['clickhouse']['http_port']}"
        CH_USER = config["clickhouse"]["user"]
        CH_PASSWD = config["clickhouse"]["password"]
        BASE_HEADERS = {
            "Content-Type": "application/json"
        }
except (FileNotFoundError, TypeError, KeyError):
    error_message = "Corrupted config or config file not found. Please, place your config to the path: " \
            "~/.config/ch_cleaner.yaml or use --config <file>"
    raise RuntimeError(error_message)


class Color:
    """This class implements string coloring for standard output."""

    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGNETA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GREY = "\033[90m"
    END = "\033[0m"

    @staticmethod
    def make(target: [str, list], color="white") -> [str, list]:
        """Colorize text and list objects.

        Available text colors:
          red, green, yellow, blue, magenta, cyan, white, grey.

        """
        if color.upper() not in dir(Color):
            raise ValueError("Unknown color received.")

        if target is None or not target:
            return target

        color = getattr(Color, color.upper())
        end = getattr(Color, "END")

        if isinstance(target, list):
            colored_list = list()
            for obj in target:
                colored_list.append(f"{color}{obj}{end}")
            return colored_list

        return f"{color}{target}{end}"


def execute_sql(expression: str, result_format: str = "json") -> [str, bool, dict]:
    """Execute SQL-query on ClickHouse server."""
    url = f"{ENDPOINT}/?user={CH_USER}&password={CH_PASSWD}&query={expression} FORMAT JSON"
    r = requests.post(url, headers=BASE_HEADERS)
    status = True if r.ok else False

    if not r.ok:
        raise RuntimeError(f"\nAn error occurred with the query: {expression}. \nHTTP {r.status_code}, Details: {r.text}")

    if result_format == "json":
        return r.json().get("data", status)
    return r.text or status


def get_data(prefix: str, key: str, database: str, table: str) -> tuple:
    """Return unique matches for the prefix or None if matches not found."""
    query = f"SELECT DISTINCT {key} " \
            f"FROM {database}.{table} " \
            f"WHERE match({key}, '^{prefix}')"

    result = [record[key] for record in execute_sql(query)]
    matches = "\n".join(f"- {path}" for path in result)

    if not result:
        message = Color.make(f"No matches were found for the prefix '{prefix}'", "grey")
        matches = None
    else:
        message = Color.make(f"\nMatches found for the prefix '{prefix}' (unique keys count: {len(result)})", "green")

    return message, matches


def delete_data(prefix: str, key: str, database: str, table: str) -> None:
    """Deletes entries that fall under the expression.
    Changes (UPDATE, DELETE) in Clickhouse start the mutation process.
    Read the documentation.
    """
    query = f"ALTER TABLE {database}.{table} DELETE WHERE match({key}, '^{prefix}')"
    metadata = f"source={database}.{table} key={key}, prefix={prefix}"

    if execute_sql(query, result_format="text"):
        print(metadata, Color.make(f"- OK", "green"))
    else:
        print(Color.make(f"Something went wrong... [{metadata}]", "red"))


def mutation_status(data: list) -> tuple:
    """Converts the list of mutations to a string as status."""
    in_progress = len([i for i in data if int(i.get("parts_to_do")) > 0])
    total = len(data)
    completed = len([i for i in data if i.get("is_done") == 1])
    failed = len([i for i in data if i.get("latest_failed_part") != ""])
    return in_progress, total, completed, failed


def check_mutations(database: str,
                    table: str,
                    await_complete: bool = False,
                    pretty: bool = True) -> None:
    """Outputs information about today mutations for the table."""
    today = datetime.today().strftime("%Y-%m-%d")
    query = f"SELECT * FROM system.mutations " \
            f"WHERE database='{database}' " \
            f"AND table='{table}' AND toDate(create_time) = '{today}'"

    result = execute_sql(query)
    in_progress, total, completed, failed = mutation_status(result)

    if await_complete:
        print(Color.make("\nWaiting for the mutation to complete...", "grey"))

        while in_progress != 0 and failed == 0:
            result = execute_sql(query)
            in_progress, total, success, failed = mutation_status(result)
            time.sleep(0.5)

    if failed > 0:
        print(Color.make("One of the mutations failed with an error. Use clickhouse-client for details.", "red"))

    print(Color.make(f"\nMutation status for '{database}.{table}'", "cyan"))
    print("-" * 40)
    if pretty:
        print(f"In Progress: {in_progress} \nFailed: {failed}\n")
    else:
        print(f"In Progress: {in_progress} \nCompleted: {completed} \nFailed: {failed} \nTotal: {total}\n")


def run(prefix: str,
        key: str,
        database: str,
        table: str,
        force_delete: bool = False,
        await_complete: bool = False) -> None:
    """Starts a prefix match search and checks for mutations.
    The mutation is triggered even if no matches are found.
    This is a feature of the database.
    """
    if force_delete:
        try:
            delete_data(prefix, key, database, table)
        except RuntimeError as error:
            print(Color.make(error, "red"))
        finally:
            print(Color.make(f"Started a mutation for delete match({key}, '^{prefix}') in {database}.{table}", "grey"))
            return

    message, matches = get_data(prefix, key, database, table)
    if not matches:
        print(message, sep="")
        return

    print(message, matches, sep="\n")
    warning_message = Color.make("Do you want to delete them? [y/n]: ", "red")

    if input(warning_message).lower() in ["y", "yes", "да", "д"]:
        try:
            delete_data(prefix, key, database, table)
            check_mutations(database, table, await_complete=await_complete)
        except RuntimeError as error:
            print(Color.make(error, "red"))
    else:
        print(Color.make(f"Deletion canceled for the prefix '{prefix}'", "red"))
        return


def main() -> None:
    prefixes = args.prefix.split(",")
    database = args.database or config.get("clickhouse", {}).get("database")
    tables = args.table.split(",")
    match_key = args.key or config.get("clickhouse", {}).get("match_key")
    await_mutation_end = True if args.await_mutation_end else False

    if not args.table:
        raise RuntimeError("Table required, but no received. Use --table arg or --help")
    if not prefixes and not args.checkout_only:
        raise RuntimeError("Prefix required, but not received. Use --prefix arg or --help")
    if args.force and args.checkout_only or args.await_mutation_end and args.force:
        raise RuntimeError("'--force', '--await-mutation-end' and '--checkout-only' can't be passed together.")

    if args.checkout_only:
        for table in tables:
            check_mutations(database, table, pretty=False)
        return

    for prefix in prefixes:
        for table in tables:
            run(prefix, match_key, database, table, force_delete=args.force, await_complete=await_mutation_end)

    if args.force:
        print("-" * 3, "For check the mutation status use the argument '--checkout-only' or '-S'", sep="\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(Color.make("\nExit", "red"))
