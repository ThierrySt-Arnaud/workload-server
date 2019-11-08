import asyncio
import argparse
from workload_server import wl_db, rfw_tcp_server

LOCAL_IP = "127.0.0.1"

parser = argparse.ArgumentParser(description="Listens and responds to properly formatted RFWs. If not specified, "
                                             "server will listen to port 8888 on all available interfaces.")
parser.add_argument("-l", "--local", help="run the server locally",
                    action="store_true")
parser.add_argument("--skipdb", help="skip database initialization",
                    action="store_true")
parser.add_argument("-p", "--port", type=int,
                    help="specify a port to listen on")


async def main(args):
    if not args.skipdb:
        wl_db.initialize_database()

    ip = "0.0.0.0"
    if args.local:
        print("Starting server for local connections")
        ip = LOCAL_IP
    else:
        print("Starting server on all interfaces")

    port = 8888
    if args.port:
        port = args.port

    async with await rfw_tcp_server.start_rfw_server(host=ip, port=port) as server:
        await server.serve_forever()

if __name__ == "__main__":
    parsed = parser.parse_args()
    try:
        asyncio.run(main(parsed))
    except KeyboardInterrupt:
        print("Exiting server")
        exit(0)
