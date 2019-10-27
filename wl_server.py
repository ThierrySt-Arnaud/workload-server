import asyncio
from workload_server import wl_db, rfw_tcp_server
import requests

IPIFY_URL = 'https://api.ipify.org'


async def main():
    wl_db.initialize_database()
    ip = requests.get('https://api.ipify.org').text
    async with await rfw_tcp_server.start_rfw_server(host=ip) as server:
        await server.serve_forever()

if __name__ == "__main__":
    print("Starting server")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting server")
        exit(0)
