from contextlib import closing
import aiosqlite
import requests
import csv
from io import StringIO

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


def initialize_database():
    try:
        with closing(aiosqlite.connect(DB)) as con, con, closing(con.cursor()) as cur:
            cur.execute(f"SELECT * FROM {TABLE}")
            if cur.fetchone() is None:
                __populate_db()
            else:
                print("Database already populated, continuing")
    except aiosqlite.OperationalError as err:
        if f"no such table: {TABLE}" in str(err):
            __populate_db()
        else:
            raise err


def __populate_db():
    print(f"Database is empty, populating...")
    with closing(aiosqlite.connect(DB)) as con:
        if con is not None:
            __create_table(con)
            for filename in (DVD_TEST_FILE, DVD_TRAIN_FILE, ND_TEST_FILE, ND_TRAIN_FILE):
                print(f"Getting file {filename}")
                request = requests.get(SOURCE_URL + SOURCE_REPO + DATA_DIR + filename)
                if request.status_code == 200:
                    csv_file = csv.reader(StringIO(request.text))
                    print(f"Inserting file {filename} in database")
                    __insert_file(con, filename, csv_file)
                else:
                    print(f"Unable to get {filename}: Error {request.status_code}")
            print("Database populated")
        else:
            print(f"Unable to open or create {DB}")


def __create_table(con):
    with con:
        con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} ("
                    f"id INTEGER PRIMARY KEY,"
                    f"{COLUMNS[0]} INTEGER,"
                    f"{COLUMNS[1]} INTEGER,"
                    f"{COLUMNS[2]} INTEGER,"
                    f"{COLUMNS[3]} REAL,"
                    f"{COLUMNS[4]} TEXT)")


def __insert_file(con, filename, csv_file):
    csv_file.__next__()
    with con:
        con.executemany(f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES (?, ?, ?, ?, ?)",
                        [(int(CPU), int(Net_in), int(Net_out), float(Memory), filename.replace(".csv", ""))
                         for (CPU, Net_in, Net_out, Memory, Target) in csv_file])


def get_batch(wanted_col, wanted_source, batch_size, batch_number):
    selected_col = [COLUMNS[n] for n in range(4) if (wanted_col & (1 << n))]
    if not selected_col:
        return None
    async with aiosqlite.connect(DB) as con:
        async with con.execute(f"SELECT ({', '.join(selected_col)}) FROM {TABLE} WHERE "
                               f"{COLUMNS[4]} LIKE ? LIMIT ? OFFSET ?;",
                               (wanted_source+"%", batch_size, batch_size*(batch_number-1))) as cur:
            return await cur.fetchall()
