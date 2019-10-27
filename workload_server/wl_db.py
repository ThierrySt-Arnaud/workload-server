from io import StringIO
from typing import Optional, Iterable, Tuple, List
import requests
import csv
import sqlite3
import aiosqlite
from contextlib import closing

DB = "workload.db"
SOURCE_URL = "https://raw.githubusercontent.com/"
SOURCE_REPO = "haniehalipour/Online-Machine-Learning-for-Cloud-Resource-Provisioning-of-Microservice-Backend-Systems/"
DATA_DIR = "master/Workload%20Data/"
TABLE = "workload"
DVD_TEST_FILE = "DVD-testing.csv"
DVD_TRAIN_FILE = "DVD-training.csv"
ND_TEST_FILE = "NDBench-testing.csv"
ND_TRAIN_FILE = "NDBench-training.csv"
COLUMNS = ("cpu", "net_in", "net_out", "memory", "source")


def initialize_database() -> None:
    """Checks if database is populated, populating it if it isn't"""

    try:
        # Open an auto-closing, auto-committing connection to database
        with closing(sqlite3.connect(DB)) as con, con:
            # Open an auto-closing cursor to database
            with closing(con.cursor()) as cur:
                cur.execute(f"SELECT * FROM {TABLE}")
                if cur.fetchone() is None:
                    __populate_db()
                else:
                    print("Database already populated, continuing")
    except sqlite3.OperationalError as err:
        # If table is not found, populated db
        if f"no such table: {TABLE}" in str(err):
            __populate_db()
        # Any other exception is an actual problem
        else:
            raise err


def __populate_db() -> None:
    """Creates table, downloads files then inserts them in database"""
    print(f"Database is empty, populating...")

    # Open an auto-closing connection to database
    with closing(sqlite3.connect(DB)) as con:
        __create_table(con)

        for filename in (DVD_TEST_FILE, DVD_TRAIN_FILE, ND_TEST_FILE, ND_TRAIN_FILE):
            print(f"Getting file {filename}")
            csv_file = __download_csv_file(filename)
            if csv_file is not None:
                print(f"Inserting file {filename} in database")
                __insert_file(con, filename, csv_file)
        print("Database populated")


def __create_table(con: sqlite3.Connection) -> None:
    """
    Creates a table in received database using parameters defined in TABLE and COLUMNS

    :param con: Opened connection to database
    """

    # Make connection auto-committing
    with con:
        # Query can be built safely from f-string evaluation because
        # all variables used here are local constant string literals
        con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} ("
                    f"id INTEGER PRIMARY KEY,"
                    f"{COLUMNS[0]} INTEGER,"
                    f"{COLUMNS[1]} INTEGER,"
                    f"{COLUMNS[2]} INTEGER,"
                    f"{COLUMNS[3]} REAL,"
                    f"{COLUMNS[4]} TEXT)")


def __download_csv_file(filename: str) -> Optional[Iterable[Tuple]]:
    """
    Attempts to download the specified file the predefined server and parse it as CSV

    :param filename: Name of the file to download
    :return: CSV parsed file reader or None
    """

    request = requests.get(SOURCE_URL + SOURCE_REPO + DATA_DIR + filename)
    if request.status_code == 200:
        return csv.reader(StringIO(request.text))
    else:
        print(f"Unable to get {filename}: Error {request.status_code}")
        return None


def __insert_file(con: sqlite3.Connection, filename: str, csv_file: Iterable[Tuple]) -> None:
    """
    Insert all rows of the received csv file into the received database

    :param con: Opened connection to database
    :param filename: Name of the file to download
    :param csv_file: CSV parsed file
    """

    csv_iterator = iter(csv_file)

    # Skip the header values
    next(csv_iterator)
    # Make connection auto-committing
    with con:
        # Query can use f-string evaluation safely for TABLE and COLUMNS because they are local constant
        # string literals but NOT for VALUES, so we use placeholders to make sure input is sanitized
        con.executemany(f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES (?, ?, ?, ?, ?)",
                        # Actual values are built as list using list comprehension to format it for input
                        [(int(CPU), int(Net_in), int(Net_out), float(Memory), filename.replace(".csv", ""))
                         for (CPU, Net_in, Net_out, Memory, Target) in csv_iterator])


async def get_batch(bench_type: str, wl_metrics: int, batch_unit: int, batch_id: int) -> Optional[List[aiosqlite.Row]]:
    """
    Asynchronous coroutine that returns up to batch_unit metrics matching bench_type

    :param bench_type: String representing the files to get samples from (expects "DVD-training" or "NDBench-test")
    :param wl_metrics: value to enable the columns bitwise (expects between 1 and 15)
    :param batch_unit: value representing the number of samples to return
    :param batch_id: value representing the current batch used to calculate offset
    :return: Iterator of matching rows containing up to batch_unit values
    """

    selected_col = [COLUMNS[n] for n in range(len(COLUMNS)-1) if (wl_metrics & (1 << n))]
    if not selected_col:
        return None

    async with aiosqlite.connect(DB) as con:
        # Query can use f-string evaluation safely for TABLE and COLUMNS because they are local constant
        # string literals but NOT for VALUES, so we use placeholders to make sure input is sanitized
        con.row_factory = sqlite3.Row
        async with await con.execute(f"SELECT {', '.join(selected_col)} FROM {TABLE} WHERE "
                                     f"{COLUMNS[-1]} LIKE ? LIMIT ? OFFSET ?;",
                                     (bench_type+"%", batch_unit, batch_unit * batch_id)) as cur:
            return await cur.fetchall()
