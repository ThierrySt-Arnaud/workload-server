import random
import asyncio
from workload_client import async_filewriter, rfw_tcp_client

file_writers = []
connections = []


async def main():
    queue = asyncio.Queue()
    listener = asyncio.create_task(queue_listener(queue))

    try:
        await input_listener(queue)
    except KeyboardInterrupt:
        print("Quitting RFW client")
    finally:
        await asyncio.gather(*connections)
        await queue.join()
        listener.cancel()
        await asyncio.gather(*file_writers)


async def queue_listener(queue: asyncio.Queue):
    while True:
        new_batch = await queue.get()
        new_writer = async_filewriter.AsyncFilewriter(new_batch.rfw_id,
                                                      new_batch.bench_type,
                                                      new_batch.batch_id,
                                                      new_batch.keys,
                                                      new_batch.data)
        file_writers.append(asyncio.create_task(new_writer.run()))
        queue.task_done()


async def input_listener(queue):
    random.seed()
    rfw_id = random.getrandbits(32)
    protocol = "JSON"
    bench_type = "DVD-Training"
    metrics = 13
    batch_unit = 10
    batch_id = 0
    batch_size = 5

    new_connection = rfw_tcp_client.RfwTcpClient(queue=queue,
                                                 rfw_id=rfw_id,
                                                 protocol=protocol,
                                                 bench_type=bench_type,
                                                 metrics=metrics,
                                                 batch_unit=batch_unit,
                                                 batch_id=batch_id,
                                                 batch_size=batch_size)

    connections.append(asyncio.create_task(new_connection.run()))
    await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())

