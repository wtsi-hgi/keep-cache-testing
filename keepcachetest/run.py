import json
import logging

import requests
from monotonic import monotonic

from arvados import KeepBlockCacheWithBlockStore
from arvados.keepcache.block_store import BookkeepingBlockStore, \
    InMemoryBlockStore
from arvados.keepcache.block_store_bookkeepers import \
    InMemoryBlockStoreBookkeeper
from keepcachetest.block_access import BlockGenerator
from keepcachetest.json_converters import record_to_json

_simulator_api = "http://localhost:8080"
_block_request_generator = BlockGenerator(_simulator_api)
_block_store = BookkeepingBlockStore(
    # LMDBBlockStore("/tmp/lmdb", cache_size),
    InMemoryBlockStore(),
    # SqlBlockStoreBookkeeper("sqlite:////var/folders/kt/b0m4btrs1h773j_1z_x64pkc000g3d/T/tmp9IrtVx")
    InMemoryBlockStoreBookkeeper()
)
_cache = KeepBlockCacheWithBlockStore(_block_store, 1 * 1024 * 1024 * 1024)


def run():
    i = 0
    while i < 10:
        i += 1
        block = _block_request_generator.get_next_block()
        logging.info("Next requested (%d) block has hash: %s" % (i, block.hash))
        slot = _cache.get(block.hash)
        if slot is None:
            logging.info("Cache miss")
            slot, _ = _cache.reserve_cache(block.hash)
            slot.set(block.contents)
        else:
            logging.info("Cache hit")
        _cache.cap_cache()
        start = monotonic()
        cache_size = _cache.block_store.bookkeeper.get_active_storage_size()
        logging.info("Cache size = %d bytes (calculated in %fs" % (cache_size, monotonic() - start))

    print(json.dumps({
        "references": json.loads(requests.get("%s/v1/blockfile/references" % _simulator_api).text),
        "non-references": json.loads(requests.get("%s/v1/blockfile/non-references" % _simulator_api).text),
        "records": [record_to_json(record) for record in _cache.block_store.bookkeeper.get_all_records()]
    }, indent=4))


if __name__ == "__main__":
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.root.setLevel(logging.DEBUG)
    run()
