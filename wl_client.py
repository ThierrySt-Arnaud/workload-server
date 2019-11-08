import argparse
import random
from collections import namedtuple
from typing import List
from ipaddress import ip_address
import asyncio
import io
import csv
from workload_client.rfw_tcp_client import RfwTcpClient
from workload_client.async_filewriter import AsyncFilewriter

file_writers = []
connections = []

PROTOCOL = "JSON"
BENCH_TYPE = "DVD-training"
METRICS = "cpu+netin+memory"
BATCH_UNIT = 100
BATCH_ID = 0
BATCH_SIZE = 5

REQUEST_FILE = "requests.csv"

REMOTE_IP = "54.81.139.246"
REMOTE_PORT = 8888

request = namedtuple("REQUEST", ["protocol", "bench_type", "metrics", "batch_unit", "batch_id", "batch_size"])


async def main():
    queue = asyncio.Queue()
    listener = asyncio.create_task(queue_listener(queue))
    try:
        await dispatcher(queue)
    finally:
        await asyncio.gather(*connections)
        await queue.join()
        listener.cancel()
        await asyncio.gather(*file_writers)


async def queue_listener(queue: asyncio.Queue):
    while True:
        new_batch = await queue.get()
        new_writer = AsyncFilewriter(rfw_id=new_batch.rfw_id,
                                     source=new_batch.bench_type,
                                     batch_id=new_batch.batch_id,
                                     columns=new_batch.keys,
                                     data=new_batch.data)
        file_writers.append(asyncio.create_task(new_writer.run()))
        queue.task_done()


async def dispatcher(queue: asyncio.Queue):
    """

    :param queue:
    :return:
    """
    random.seed()

    requests = []
    if args.src is not None:
        if "batch" in args.src:
            requests = parse_request_file(args.filename)
        else:
            metrics = parse_metrics(args.metrics)
            if metrics > 0:
                requests.append(request(protocol=args.protocol,
                                        bench_type=args.bench_type,
                                        metrics=metrics,
                                        batch_unit=args.batch_unit,
                                        batch_id=args.batch_id,
                                        batch_size=args.batch_size))
    else:
        print("No command specified. Please select 'batch' or 'single'. Use -h for more information")

    if args.local:
        host = "127.0.0.1"
    else:
        host = str(args.hostip)

    port = args.port

    for r in requests:
        rfw_id = random.getrandbits(32)
        new_connection = RfwTcpClient(queue=queue,
                                      rfw_id=rfw_id,
                                      protocol=r.protocol,
                                      bench_type=r.bench_type,
                                      metrics=r.metrics,
                                      batch_unit=r.batch_unit,
                                      batch_id=r.batch_id,
                                      batch_size=r.batch_size,
                                      host=host,
                                      port=port)
        connections.append(asyncio.create_task(new_connection.run()))


def parse_request_file(filename) -> List[request]:
    with io.open(filename) as file:
        csv_reader = csv.DictReader(file)
        requests = []
        wrong_formats = 0
        for row in csv_reader:
            try:
                metrics = parse_metrics(row["metrics"])
                if metrics > 0:
                    requests.append(request(protocol=row["protocol"],
                                            bench_type=row["bench_type"],
                                            metrics=metrics,
                                            batch_unit=int(row["batch_unit"]),
                                            batch_id=int(row["batch_id"]),
                                            batch_size=int(row["batch_size"])))
                else:
                    wrong_formats += 1

            except KeyError:
                wrong_formats += 1

        if wrong_formats > 0:
            print(f"Unable to add {wrong_formats} requests")

        if not requests:
            print(f'No readable value in {filename}')
        return requests


def parse_metrics(input_metrics: str) -> int:
    metrics = 8 if "cpu" in input_metrics.lower() else 0
    metrics += 4 if "networkin" in input_metrics.lower() or "netin" in input_metrics.lower() else 0
    metrics += 2 if "networkout" in input_metrics.lower() or "netout" in input_metrics.lower() else 0
    metrics += 1 if "memory" in input_metrics.lower() else 0
    return metrics


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    src_parsers = parser.add_subparsers(dest="src")

    # Arguments for server connection
    locrem = parser.add_mutually_exclusive_group()
    locrem.add_argument("-ip", "--hostip", type=ip_address, default=REMOTE_IP,
                        help="specify IP address, "
                             "defaults to cloud instance")
    locrem.add_argument("--local", action="store_true",
                        help="connect to a locally running server, "
                             "equivalent to --hostip 127.0.0.1")
    parser.add_argument("-p", "--port", type=int, default=REMOTE_PORT,
                        help=f"specify port\ndefaults to {REMOTE_PORT}")

    # Arguments for csv formatted batch file
    batch_parser = src_parsers.add_parser("batch")
    batch_parser.add_argument("filename", nargs='?', type=str, default=REQUEST_FILE,
                              help=f"CSV files containing RFWs, defaults to {REQUEST_FILE}   "
                                   f"[SEE {REQUEST_FILE} FOR FORMAT EXAMPLE]")

    # Arguments for single RFWs
    single_parser = src_parsers.add_parser("single")
    single_parser.add_argument("protocol", choices=["JSON", "BUFF"], nargs="?", default=PROTOCOL,
                               help=f"protocol of the RFW, defaults to {PROTOCOL}")

    single_parser.add_argument("bench_type", choices=["DVD-testing", "DVD-training",
                                                      "NDBench-testing", "NDBench-training"],
                               nargs="?", default=BENCH_TYPE, help=f"bench type of the RFW, defaults to {BENCH_TYPE}")

    single_parser.add_argument("metrics", nargs="?", default=METRICS,
                               help=f"metrics for the RFW, defaults to {METRICS}")

    single_parser.add_argument("batch_unit", type=int, nargs="?", default=BATCH_UNIT,
                               help=f"batch_unit size of the RFW, defaults to {BATCH_UNIT}")

    single_parser.add_argument("batch_id", type=int, nargs="?", default=BATCH_ID,
                               help=f"first batch id of the RFW, defaults to {BATCH_ID}")

    single_parser.add_argument("batch_size", type=int, nargs="?", default=BATCH_SIZE,
                               help=f"number of batches of the RFW, defaults to {BATCH_SIZE}")
    return parser


if __name__ == "__main__":
    parser = setup_arg_parser()
    args = parser.parse_args()
    # print(args)
    try:
        asyncio.run(main())
        print("All batches received successfully")
    except KeyboardInterrupt:
        print("Quitting RFW client")
    except ConnectionRefusedError:
        print("Server unreachable")
    except TimeoutError:
        print("Connection timed out")
