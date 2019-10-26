import asyncio
from workload_server import wl_db, rfw_tcp_server


async def main():
    wl_db.initialize_database()
    async with await rfw_tcp_server.start_rfw_server() as server:
        await server.serve_forever()

if __name__ is "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting server")
        exit(0)
