import json
import logging
import sys
import traceback
from threading import Semaphore

import requests
from concurrent.futures import ThreadPoolExecutor
from monotonic import monotonic

from arvados.keepcache.block_store import BookkeepingBlockStore, \
    InMemoryBlockStore, LMDBBlockStore
from arvados.keepcache.block_store_bookkeepers import \
    SqlBlockStoreBookkeeper, InMemoryBlockStoreBookkeeper
from arvados.keepcache.caches import KeepBlockCacheWithBlockStore
from arvados.keepcache.replacement_policies import LastUsedReplacementPolicy
from keepcachetest.block_access import BlockGenerator
from keepcachetest.helpers import create_temp_directory
from keepcachetest.json_converters import record_to_json

_ONE_KB = 1 * 1024
_ONE_MB = 1 * 1024 * _ONE_KB
_ONE_GB = 1 * 1024 * _ONE_MB

iterations = 500
max_threads = 10
cache_size = 2 * _ONE_GB
simulator_api = "http://localhost:8080"
block_store = BookkeepingBlockStore(
    LMDBBlockStore(create_temp_directory(), cache_size, max_readers=126),
    # RocksDBBlockStore(create_temp_directory()),
    # DiskOnlyBlockStore(create_temp_directory()),
    # InMemoryBlockStore(),
    # InMemoryBlockStoreBookkeeper()
    SqlBlockStoreBookkeeper("sqlite:///%s/database.db" % create_temp_directory())
)
cache = KeepBlockCacheWithBlockStore(
    block_store,
    int(block_store._block_store.calculate_usuable_size() * 0.5),
    # cache_size,
    cache_replacement_policy=LastUsedReplacementPolicy())

_block_request_generator = BlockGenerator(simulator_api)
_complete = Semaphore(0)


def use_cache():
    messages = []
    try:
        block = _block_request_generator.get_next_block()
        messages.append("Next requested block has hash: %s" % block.hash)
        slot = cache.get(block.hash)
        if slot is None:
            messages.append("Cache miss")
            slot, _ = cache.reserve_cache(block.hash)
            slot.set(block.contents)
        else:
            messages.append("Cache hit")

        cache.cap_cache()
        cache_size = cache.block_store.bookkeeper.get_active_storage_size()
        messages.append("Current cache size = %d bytes" % cache_size)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        full_error_message = repr(traceback.format_exception(
            exc_type, exc_value, exc_traceback))
        messages.append(full_error_message)
    finally:
        logging.info(messages)
        _complete.release()


def run():
    run_start = monotonic()
    executor = ThreadPoolExecutor(max_workers=max_threads)
    for i in range(iterations):
        executor.submit(use_cache)
    print("Submitted all request jobs")

    count = 0
    while count < iterations:
        _complete.acquire()
        count += 1
        logging.info("%d requests completed" % count)
    executor.shutdown()

    print(json.dumps({
        "time_taken": monotonic() - run_start,
        "references": json.loads(requests.get("%s/v1/blockfile/references" % simulator_api).text),
        "non-references": json.loads(requests.get("%s/v1/blockfile/non-references" % simulator_api).text),
        "records": [record_to_json(record) for record in cache.block_store.bookkeeper.get_all_records()]
    }, indent=4))


if __name__ == "__main__":
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("arvados.keepcache.block_store").setLevel(logging.ERROR)
    logging.root.setLevel(logging.DEBUG)
    run()

