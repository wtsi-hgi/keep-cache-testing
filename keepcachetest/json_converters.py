from arvados.keepcache.block_store_bookkeepers import BlockPutRecord, \
    BlockDeleteRecord, BlockGetRecord

_record_type_map = {
    BlockPutRecord: "put",
    BlockGetRecord: "get",
    BlockDeleteRecord: "delete"
}


def records_to_json(records):
    return [record_to_json(record) for record in records]


def record_to_json(record):
    record_type = _record_type_map[record.__class__.__bases__[-1]]

    json_dict = {
        "type": record_type,
        "hash": record.locator,
        "timestamp": record.timestamp.isoformat()
    }
    if isinstance(record, BlockPutRecord):
        json_dict["size"] = record.size
    return json_dict
