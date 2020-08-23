#!/usr/bin/env python3

import os
import sys
import yaml
import time
import argparse
import requests
import logging
import configparser

parser = argparse.ArgumentParser(
    prog="main.py",
    description="ClickHouse graphite metrics cleaner",
    formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=90)
)
commands = parser.add_argument_group("Commands")
commands.add_argument("--prefix", "-p", metavar="str [, ...]", default="", type=str, help="Path prefix for search matches")
commands.add_argument("--key", "-k", metavar="str", type=str, help="Primary key (column) for prefix mathces")
commands.add_argument("--database", "-d", metavar="str", type=str, help="Database to connect")
commands.add_argument("--table", "-t", metavar="str", type=str, required=True, help="Table for search matches")
commands.add_argument("--checkout-only", action="store_true", help="Print only mutation status for table")
commands.add_argument("--await-mutation-end", action="store_true", help="Lock script execution until the mutation completes")
commands.add_argument("--config", metavar="file", type=str, required=False, help="Custom path to config file in yaml format")
args = parser.parse_args()

logger = logging.getLogger(__name__)
homedir = os.path.expanduser('~')
config_dir = os.path.join(homedir, '.config')
config_file = args.config or os.path.join(config_dir, 'ch_cleaner.yaml')

try:
    with open(config_file, 'r') as cfgfile:
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
    def make(target: [str, list], color="white"):
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


def execute_sql(expression: str, result_format: str = "json") -> [str, dict]:
    """Execute SQL-query on ClickHouse server."""
    # ?user={CH_USER}&password={CH_PASSWD}&
    url = f"{ENDPOINT}/?query={expression} FORMAT JSON"
    r = requests.post(url, headers=BASE_HEADERS)
    logger.debug(r.text)

    if not r.ok:
        raise RuntimeError(f"\nAn error occurred with the query: {expression}. \nHTTP {r.status_code}, Details: {r.text}")

    if result_format == "json":
        return r.json().get("data", None) 
    return r.text


def get_data(prefix: str, key: str, database: str, table: str) -> str:
    """Return unique matches for the prefix."""
    query = f"SELECT DISTINCT {key} " \
            f"FROM {database}.{table} " \
            f"WHERE match(Path, '^{prefix}')"

    result = [record["Path"] for record in execute_sql(query)]
    paths = "\n".join(f"- {path}" for path in result)

    if not result:
        return
    return f"Matches found for the prefix '{prefix}': \n{paths}"


def delete_data(prefix: str, key: str, database: str, table: str) -> None:
    """Deletes entries that fall under the expression."""
    query = f"ALTER TABLE {database}.{table} DELETE WHERE match ({key}, '^{prefix}')"
    execute_sql(query, result_format="text")


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
    """Outputs information about all mutations for the table."""
    query = f"SELECT * FROM system.mutations " \
            f"WHERE database='{database}' AND table='{table}'"

    result = execute_sql(query)
    in_progress, total, completed, failed = mutation_status(result)

    if await_complete:
        print(Color.make("\nWaiting for the mutation to complete...", "grey"))

        while not in_progress == 0 and failed == 0:
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
        check_only: bool = False,
        await_complete: bool = False) -> None:
    """Starts a prefix match search and checks for mutations."""
    if check_only:
        return check_mutations(database, table, pretty=False)

    result = get_data(prefix, key, database, table)
    if not result:
        print(Color.make(f"No matches were found for the prefix '{prefix}'", "grey"), sep="")
        return
    else:
        print("\n", Color.make(result, "green"), sep="")

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
    table = args.table
    match_key = args.key or config.get("clickhouse", {}).get("match_key")
    await_mutation_end = True if args.await_mutation_end else False

    if not args.table:
        raise RuntimeError("Table required, but no received. Use --table arg or --help")

    if not prefixes and not args.checkout_only:
        raise RuntimeError("Prefix required, but not received. Use --table arg or --help")

    if args.checkout_only:
        run("", match_key, database, table, check_only=True, await_complete=False)
        return

    for prefix in prefixes:
        run(prefix, match_key, database, table, await_complete=await_mutation_end)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(Color.make("\nExit", "red"))
