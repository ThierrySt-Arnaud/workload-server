from typing import Optional, List
from collections import namedtuple
import asyncio
import aiofiles
import os

BATCHES_FOLDER = "batches"

try:
    os.mkdir(BATCHES_FOLDER)
except FileExistsError:
    pass


class AsyncFilewriter:
    def __init__(self, rfw_id: int, source: str, batch_id: int, columns: List[str], data: List[List]):
        """

        :param rfw_id:
        :param source:
        :param batch_id:
        :param columns:
        :param data:
        """

        self.filename = str(rfw_id) + "-" + source + "-" + str(batch_id) + ".csv"
        self.columns = columns
        self.data = data

    async def run(self) -> None:
        async with aiofiles.open(BATCHES_FOLDER + '/' + self.filename, 'w') as file:
            await file.write(f"{','.join(self.columns)}\n")
            await file.writelines([f"{','.join([str(item) for item in line])}\n" for line in self.data])
