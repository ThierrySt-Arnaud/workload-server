import json
from collections import namedtuple
import asyncio
from workload_server import wl_db

HOST = "127.0.0.1"
PORT = 8888
clients = {}
rfw = namedtuple("RFW", ["rfw_id", "bench_type", "wl_metric", "batch_unit", "batch_id", "batch_size"])


async def rfw_handler(reader, writer):
    received = await reader.read()
    rfw_dict = None
    remote_addr = writer.get_extra_info('peername')

    if remote_addr in clients:
        rfw_id = clients[remote_addr]
    else:
        rfw_id = 0
        clients[remote_addr] = 0

    try:
        rfw_dict = json.loads(received)
        n_rfw = rfw(
            rfw_id=rfw_dict["rfw_id"],
            bench_type=rfw_dict["bench_type"],
            wl_metric=rfw_dict["wl_metric"],
            batch_unit=rfw_dict["batch_unit"],
            batch_id=rfw_dict["batch_id"],
            batch_size=rfw_dict["batch_size"]
        )
        if rfw_id+1 == n_rfw.rfw_id:
            clients[remote_addr] = n_rfw.rfw_id
            print(f"Received request for workload #{n_rfw.rfw_id} from {remote_addr!r}")
            rfd = prepare_response(n_rfw)
        else:
            print(f"Out of sequence with {remote_addr!r}")
    except json.JSONDecodeError:
        print(f"Unable to decode received data from {remote_addr!r}")
    except KeyError:
        print(f"Wrong json format from {remote_addr!r}")

    writer.close()


async def prepare_response(n_rfw):
    requested_data = await wl_db.get_batch(n_rfw.bench_type, n_rfw.wl_metric,
                                           n_rfw.batch_unit,  n_rfw.batch_id)


async def start_rfw_server():
    return await asyncio.start_server(rfw_handler, HOST, PORT)
