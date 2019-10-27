from typing import Optional, List, Any
import logging
from collections import namedtuple
import struct
import json
import asyncio
from sqlite3 import Row
from workload_server import wl_db
import workload_protocol_pb2
from google.protobuf.message import DecodeError

HOST = "127.0.0.1"
PORT = 8888

MAX_FAIL = 5

RFW_HEADER_FORMAT = "!3sI4sQ"
RFW_HEADER_SIZE = struct.calcsize(RFW_HEADER_FORMAT)
RFW_HEADER_MARKER = "RFW"

RFD_HEADER_FORMAT = "!3sII4sQ"
RFD_HEADER_MARKER = "RFD"

FAIL_MARKER = "NOP"

rfw_header = namedtuple("RFW_Header", ["protocol", "payload_size"])
rfw = namedtuple("RFW", ["bench_type", "wl_metrics", "batch_unit", "batch_id", "batch_size"])


class AsyncConnection:
    """Class encapsulating the asynchronous TCP stream"""
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """AsyncConnection

        :param reader:
        :param writer:
        """
        self.reader = reader
        self.writer = writer
        self.peer = self.writer.get_extra_info('peername')
        self.rfw_id = None
        self.failed_attempts = 0
        logging.info(f"Connection open with {self.peer[0]}:{self.peer[1]}")

    async def run(self) -> None:
        """Coroutine to handle a TCP stream asynchronously"""
        while not self.writer.is_closing():
            n_header = await self.get_header()
            if n_header is not None:
                payload = await self.get_payload(n_header.payload_size)

                if payload is not None:
                    if n_header.protocol == "JSON":
                        if await self.send_json_replies(payload):
                            self.writer.close()
                            break
                    elif n_header.protocol == "BUFF":
                        if await self.send_protobuf_replies(payload):
                            self.writer.close()
                            break

            self.writer.write(struct.pack(RFD_HEADER_FORMAT,
                                          bytes(FAIL_MARKER.encode("utf-8")),
                                          0, 0, b"\0\0\0\0", 0))
            try:
                await self.writer.drain()
            except ConnectionResetError:
                break

            if self.failed_attempts > MAX_FAIL:
                logging.error(f"Too many failed attempts from {self.peer[0]}:{self.peer[1]}, closing connection")
                self.writer.close()
        try:
            await self.writer.wait_closed()
        except BrokenPipeError:
            pass

    async def get_header(self) -> Optional[rfw_header]:
        """
        Coroutine

        :return:
        """
        try:
            header = await self.reader.readexactly(RFW_HEADER_SIZE)
        except asyncio.IncompleteReadError:
            logging.error(f"Connection with {self.peer[0]}:{self.peer[1]} closed before receiving header")
            self.failed_attempts += 1
            return None
        (marker, rfw_id, protocol, payload_size) = struct.unpack(RFW_HEADER_FORMAT, header)

        decoded_marker = marker.decode()
        if decoded_marker == RFW_HEADER_MARKER:
            decoded_protocol = protocol.decode()
            if decoded_protocol == "JSON" or decoded_protocol == "BUFF":
                if self.rfw_id is None:
                    self.rfw_id = rfw_id
                elif self.rfw_id != rfw_id:
                    logging.warning(f"Mismatching RFW ID received from {self.peer[0]}:{self.peer[1]}. "
                                    f"Expected {self.rfw_id}, got {rfw_id} instead")

                return rfw_header(protocol=decoded_protocol,
                                  payload_size=payload_size)

        logging.error(f"Invalid header received from {self.peer[0]}:{self.peer[1]}")
        self.failed_attempts += 1
        return None

    async def get_payload(self, size: int) -> bytes:
        """

        :param size:
        :return:
        """
        try:
            payload = await self.reader.readexactly(size)
        except asyncio.IncompleteReadError:
            payload = None
            logging.error(f"Connection with {self.peer[0]}:{self.peer[1]} closed before receiving payload")
            self.failed_attempts += 1
        return payload

    async def check_rfw(self, received) -> Optional[rfw]:
        """

        :param received:
        :return: RFW or NoneType
        """
        try:
            n_rfw = rfw(bench_type=received["bench_type"],
                        wl_metrics=received["wl_metrics"],
                        batch_unit=received["batch_unit"],
                        batch_id=received["batch_id"],
                        batch_size=received["batch_size"])
        except KeyError:
            self.failed_attempts += 1
            logging.error(f"Wrong json format from {self.peer[0]}:{self.peer[1]}")
            return None

        logging.info(f"Received request for workload from {self.peer[0]}:{self.peer[1]}")
        return n_rfw

    async def send_json_replies(self, payload: bytes) -> bool:
        """

        :param payload:
        :return:
        """
        try:
            new_rfw = await self.check_rfw(json.loads(payload))
        except json.JSONDecodeError:
            self.failed_attempts += 1
            logging.error(f"Unable to decode received data from {self.peer[0]}:{self.peer[1]}")
            return False

        for i in range(new_rfw.batch_size):
            curr_batch_id = new_rfw.batch_id+i
            curr_batch = await wl_db.get_batch(new_rfw.bench_type, new_rfw.wl_metrics,
                                               new_rfw.batch_unit, curr_batch_id)
            serialized = json.dumps({"keys": curr_batch[0].keys(),
                                     "data": [tuple(row) for row in curr_batch]})
            serialized_length = len(serialized)
            rfd_header = struct.pack(RFD_HEADER_FORMAT,
                                     bytes(RFD_HEADER_MARKER.encode("utf-8")),
                                     self.rfw_id,
                                     curr_batch_id,
                                     b"JSON",
                                     serialized_length)

            logging.error(f"Sending {serialized_length} bytes of batch {curr_batch_id} "
                          f"to {self.peer[0]}:{self.peer[1]}")
            self.writer.write(rfd_header)
            await self.writer.drain()
            self.writer.write(bytes(serialized.encode("utf-8")))
            await self.writer.drain()

        return True

    async def send_protobuf_replies(self, payload: bytes) -> bool:
        proto_rfw = workload_protocol_pb2.ProtoRfw()
        try:
            proto_rfw.ParseFromString(payload)
        except DecodeError:
            self.failed_attempts += 1
            logging.error(f"Unable to decode received data from {self.peer[0]}:{self.peer[1]}")
            return False

        logging.info(f"Received request for workload from {self.peer[0]}:{self.peer[1]}")

        for i in range(proto_rfw.batch_size):
            curr_batch_id = proto_rfw.batch_id + i
            curr_batch = await wl_db.get_batch(proto_rfw.bench_type, proto_rfw.wl_metrics,
                                               proto_rfw.batch_unit, curr_batch_id)
            proto_rfd = self.create_proto_rfd(curr_batch)
            serialized = proto_rfd.SerializeToString()
            serialized_length = proto_rfd.ByteSize()
            rfd_header = struct.pack(RFD_HEADER_FORMAT,
                                     bytes(RFD_HEADER_MARKER.encode("utf-8")),
                                     self.rfw_id,
                                     curr_batch_id,
                                     b"BUFF",
                                     serialized_length)

            logging.error(f"Sending {serialized_length} bytes of batch {curr_batch_id} "
                          f"to {self.peer[0]}:{self.peer[1]}")
            self.writer.write(rfd_header)
            await self.writer.drain()
            self.writer.write(serialized)
            await self.writer.drain()

        return True

    @staticmethod
    def create_proto_rfd(batch: List[Row]) -> workload_protocol_pb2.ProtoRfd:
        proto_rfd = workload_protocol_pb2.ProtoRfd()
        keys = batch[0].keys()
        proto_rfd.keys.extend(keys)
        for row in batch:
            workload = proto_rfd.workload.add()
            for i, key in enumerate(keys):
                setattr(workload, key, row[i])

        return proto_rfd


async def rfw_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """
    Asynchronous callback handler for TCP server. Initializes an AsyncConnection object

    :param reader:
    :param writer:
    """
    await AsyncConnection(reader, writer).run()


async def start_rfw_server() -> asyncio.AbstractServer:
    logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

    logging.info(f"Initializing server on {HOST}:{PORT}")
    return await asyncio.start_server(rfw_handler, HOST, PORT)
