import asyncio
from workload_server import wl_db, rfw_tcp_server

IP = "0.0.0.0"


async def main():
    wl_db.initialize_database()
    async with await rfw_tcp_server.start_rfw_server(host=IP) as server:
        await server.serve_forever()

if __name__ == "__main__":
    print("Starting server")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting server")
        exit(0)
