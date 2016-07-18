ONE_MB = 1 * 1024 * 1024
DEFAULT_BLOCK_SIZE = 64 * ONE_MB


class Block(object):
    """
    Model of a block with a hash whose contents are generated randomly if requested.
    """
    def __init__(self, hash, size=DEFAULT_BLOCK_SIZE):
        """
        Constructor.
        :param hash: this block's hash
        :type hash: str
        :param size: size of this block
        :type size: int
        """
        self.hash = hash
        self.size = size
        self._contents = None

    @property
    def contents(self):
        """
        Gets the contents of this block.
        :return: the contents
        :rtype: bytearray
        """
        if self._contents is None:
            # self._contents = bytearray(random.getrandbits(8) for _ in xrange(self.size))
            self._contents = bytearray(self.size)
        return self._contents
