from contextlib import closing
import sqlite3
import requests
import csv
from io import StringIO

DB = "workload.db"
SOURCE_URL = "https://raw.githubusercontent.com/haniehalipour/Online-Machine-Learning-for-Cloud-Resource-Provisioning-of-Microservice-Backend-Systems/"
DATA_DIR = "master/Workload%20Data/"
TABLE = "workload"
DVD_TEST_FILE = "DVD-testing.csv"
DVD_TRAIN_FILE = "DVD-training.csv"
ND_TEST_FILE = "NDBench-testing.csv"
ND_TRAIN_FILE = "NDBench-training.csv"
COLUMNS = ("cpu", "net_in", "net_out", "memory", "source")


class WorkloadDatabase:
    def __init__(self):
        try:
            with closing(sqlite3.connect(DB)) as con, con, closing(con.cursor()) as cur:
                cur.execute(f"SELECT * FROM {TABLE}")
                if cur.fetchone() is None:
                    self.__populate_db(self)
                else:
                    print("Database already populated, continuing")
        except sqlite3.OperationalError as err:
            if f"no such table: {TABLE}" in str(err):
                self.__populate_db(self)
            else:
                raise err

    @staticmethod
    def __populate_db(self):
        print(f"Database is empty, populating...")
        with closing(sqlite3.connect(DB)) as con:
            if con is not None:
                self.__create_table(con)
                for filename in (DVD_TEST_FILE, DVD_TRAIN_FILE, ND_TEST_FILE, ND_TRAIN_FILE):
                    print(f"Getting file {filename}")
                    request = requests.get(SOURCE_URL + DATA_DIR + filename)
                    if request.status_code == 200:
                        csv_file = csv.reader(StringIO(request.text))
                        print(f"Inserting file {filename} in database")
                        self.__insert_file(con, filename, csv_file)
                    else:
                        print(f"Unable to get {filename}: Error {request.status_code}")
                print("Database populated")
            else:
                print(f"Unable to open or create {DB}")

    @staticmethod
    def __create_table(con):
        with con:
            con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} ("
                        f"id INTEGER PRIMARY KEY,"
                        f"{COLUMNS[0]} INTEGER,"
                        f"{COLUMNS[1]} INTEGER,"
                        f"{COLUMNS[2]} INTEGER,"
                        f"{COLUMNS[3]} REAL,"
                        f"{COLUMNS[4]} TEXT)")

    @staticmethod
    def __insert_file(con, filename, csv_file):
        csv_file.__next__()
        with con:
            con.executemany(f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES (?, ?, ?, ?, ?)",
                            [(int(CPU), int(Net_in), int(Net_out), float(Memory), filename.replace(".csv", ""))
                             for (CPU, Net_in, Net_out, Memory, Target) in csv_file])

    @staticmethod
    def get