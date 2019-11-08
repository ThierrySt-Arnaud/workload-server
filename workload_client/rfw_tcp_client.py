import logging
from typing import Optional
from collections import namedtuple
import struct
import json
import asyncio
import workload_protocol_pb2
from google.protobuf.message import DecodeError

HOST = "127.0.0.1"
PORT = 8888

MAX_FAIL = 5

RFW_HEADER_FORMAT = "!3sI4sQ"
RFW_HEADER_MARKER = "RFW"

RFD_HEADER_FORMAT = "!3sII4sQ"
RFD_HEADER_SIZE = struct.calcsize(RFD_HEADER_FORMAT)
RFD_HEADER_MARKER = "RFD"

FAIL_MARKER = "NOP"

rfd_header = namedtuple("RFD_HEADER", ["last_batch", "protocol", "payload_size"])
batch = namedtuple("BATCH", ["rfw_id", "bench_type", "batch_id", "keys", "data"])


class RfwTcpClient:
    """

    """
    def __init__(self,
                 queue: asyncio.Queue,
                 rfw_id: int,
                 protocol: str,
                 bench_type: str,
                 metrics: int,
                 batch_unit: int,
                 batch_id: int,
                 batch_size: int,
                 host: str = HOST,
                 port: int = PORT,
                 tries: int = MAX_FAIL
                 ) -> None:
        """

        :param queue:
        :param rfw_id:
        :param protocol:
        :param bench_type:
        :param metrics:
        :param batch_unit:
        :param batch_id:
        :param batch_size:
        :param tries:
        """
        self.queue = queue
        self.rfw_id = rfw_id
        self.protocol = protocol if protocol == "BUFF" else "JSON"
        self.rfw = {"bench_type": bench_type,
                    "wl_metrics": metrics,
                    "batch_unit": batch_unit,
                    "batch_id": batch_id,
                    "batch_size": batch_size}
        self.host = host
        self.port = port
        self.retries = tries
        self.batch_rcv = 0
        self.reader = None
        self.writer = None

    async def run(self) -> None:
        """

        :return:
        """
        logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
        logging.info(f"Connecting to server on {self.host}:{self.port}")
        (reader, writer) = await asyncio.open_connection(self.host, self.port)
        self.reader, self.writer = reader, writer
        await self.send_rfw()
        await self.get_replies()
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()

    async def send_rfw(self) -> None:
        """

        :return:
        """

        logging.info(f"RFW#{self.rfw_id} - RFW is\t{self.protocol}\t{self.rfw['bench_type']}\t"
                     f"metrics: {self.rfw['wl_metrics']}\tunit: {self.rfw['batch_unit']}\t"
                     f"id: {self.rfw['batch_id']}\tsize: {self.rfw['batch_size']}")
        if self.protocol == "BUFF":
            serialized_rfw = self.create_proto_rfw().SerializeToString()
        else:
            serialized_rfw = bytes(json.dumps(self.rfw).encode("utf-8"))

        self.writer.write(struct.pack(RFW_HEADER_FORMAT,
                                      bytes(RFW_HEADER_MARKER.encode("utf-8")),
                                      self.rfw_id,
                                      bytes(self.protocol.encode("utf-8")),
                                      len(serialized_rfw)))
        logging.info(f"RFW#{self.rfw_id} - Sending {len(serialized_rfw)} bytes of the serialized RFW")
        await self.writer.drain()
        self.writer.write(serialized_rfw)
        await self.writer.drain()

    async def get_replies(self) -> bool:
        """

        :return:
        """
        while self.retries > 0 and not self.writer.is_closing():
            try:
                header = await self.reader.readexactly(RFD_HEADER_SIZE)
            except asyncio.IncompleteReadError:
                if self.batch_rcv == self.rfw["batch_size"]:
                    break
                else:
                    # TODO: Add error message
                    self.retries -= 1
                    await self.reopen_connection()
                    continue

            header = await self.check_header(header)
            if header is not None:
                if header.payload_size < 0:
                    self.retries -= 1
                    await self.send_rfw()
                    continue

                rcv_success = await self.receive_protobuf_rfd(header)\
                    if header.protocol == "BUFF"\
                    else await self.receive_json_rfd(header)

                if rcv_success:
                    if self.batch_rcv < self.rfw["batch_size"]:
                        self.batch_rcv += 1
                        logging.info(f"RFW#{self.rfw_id} - {header.payload_size} bytes of batch "
                                     f"{self.batch_rcv}/{self.rfw['batch_size']} received.")
                    else:
                        logging.warning("Unexpected batch received")
                else:
                    await self.reopen_connection()
                    self.retries -= 1
            else:
                await self.reopen_connection()
                self.retries -= 1

        if self.batch_rcv == self.rfw["batch_size"]:
            logging.info(f"RFW#{self.rfw_id} - All batches received")
            return True
        return False

    async def check_header(self, header) -> Optional[rfd_header]:
        """

        :param header:
        :return:
        """
        (marker, rfw_id, last_batch, protocol, payload_size) = struct.unpack(RFD_HEADER_FORMAT, header)
        decoded_marker = marker.decode()
        if decoded_marker == RFD_HEADER_MARKER:
            decoded_protocol = protocol.decode()
            if last_batch != self.batch_rcv + self.rfw["batch_id"]:
                logging.warning(f"Non-sequential batch received. Expected {self.batch_rcv + self.rfw['batch_id']}, "
                                f"got {last_batch} instead")
            if decoded_protocol == "BUFF" or decoded_protocol == "JSON":
                if decoded_protocol != self.protocol:
                    logging.warning(f"Mismatching protocol received. Expected {self.protocol},"
                                    f" got {decoded_protocol} instead")

                if self.rfw_id != rfw_id:
                    logging.warning(f"Mismatching RFW ID received. Expected {self.rfw_id}, got {rfw_id} instead")
                return rfd_header(last_batch=last_batch,
                                  protocol=decoded_protocol,
                                  payload_size=payload_size)

        elif decoded_marker == FAIL_MARKER:
            logging.error("Server was unable to process request")
            return rfd_header(last_batch=None,
                              protocol=None,
                              payload_size=-1)

        logging.error("Invalid data header received from server")
        return None

    async def receive_json_rfd(self, header: rfd_header) -> bool:
        """

        :param header:
        :return:
        """
        try:
            payload = await self.reader.readexactly(header.payload_size)
        except asyncio.IncompleteReadError:
            logging.error(f"Connection with server closed before receiving payload")
            return False

        try:
            decoded_rfd = json.loads(payload)
        except json.JSONDecodeError:
            logging.error("Unable to decode received data from the server")
            return False

        try:
            new_batch = batch(rfw_id=self.rfw_id,
                              bench_type=self.rfw["bench_type"],
                              batch_id=header.last_batch,
                              keys=decoded_rfd["keys"],
                              data=decoded_rfd["data"])
        except KeyError:
            logging.error("Invalid RFD received")
            return False

        if len(new_batch.data) < self.rfw["batch_unit"]:
            self.batch_rcv = self.rfw["batch_size"]
        await self.queue.put(new_batch)
        return True

    async def receive_protobuf_rfd(self, header: rfd_header) -> bool:
        """

        :param header:
        :return:
        """
        try:
            payload = await self.reader.readexactly(header.payload_size)
        except asyncio.IncompleteReadError:
            logging.error(f"Connection with server closed before receiving payload")
            return False

        decoded_rfd = workload_protocol_pb2.ProtoRfd()
        try:
            decoded_rfd.ParseFromString(payload)
        except DecodeError:
            logging.error("Unable to decode received data from the server")
            return False

        keys = decoded_rfd.keys
        new_batch = batch(rfw_id=self.rfw_id,
                          bench_type=self.rfw["bench_type"],
                          batch_id=header.last_batch,
                          keys=keys,
                          data=[[getattr(workload, key) for key in keys] for workload in decoded_rfd.workload])

        if len(decoded_rfd.workload) < self.rfw["batch_unit"]:
            self.batch_rcv = self.rfw["batch_size"]
        await self.queue.put(new_batch)
        return True

    def create_proto_rfw(self) -> workload_protocol_pb2.ProtoRfw:
        proto_rfw = workload_protocol_pb2.ProtoRfw()
        proto_rfw.bench_type = self.rfw["bench_type"]
        proto_rfw.wl_metrics = self.rfw["wl_metrics"]
        proto_rfw.batch_unit = self.rfw["batch_unit"]
        proto_rfw.batch_id = self.rfw["batch_id"]
        proto_rfw.batch_size = self.rfw["batch_size"]
        return proto_rfw

    async def reopen_connection(self):
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()
        (reader, writer) = await asyncio.open_connection(HOST, PORT)
        self.reader, self.writer = reader, writer
        self.rfw["batch_size"] -= self.batch_rcv
        self.rfw["batch_id"] += self.batch_rcv
        self.batch_rcv = 0
        await self.send_rfw()
