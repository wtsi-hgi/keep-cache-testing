import json

import requests

from keepcachetest._models import Block


class BlockGenerator:
    """
    Client for block generator.
    """
    def __init__(self, simulator_api):
        """
        Constructor.
        :param simulator_api: URL of HTTP API endpoint of the cache usage simulator
        """
        self.simulator_api = simulator_api

    def get_next_block(self):
        """
        Gets the next block to be requested.
        :return: the next block
        :rtype: Block
        """
        request = requests.get("%s/v1/block/next" % self.simulator_api)
        request.close()
        block_hash = str(json.loads(request.text))
        return Block(block_hash)
