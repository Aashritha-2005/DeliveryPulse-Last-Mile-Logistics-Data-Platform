"""Read collection and storage statistics using the restricted query identity."""

import json

from src.nosql.client import connect


def main() -> int:
    """Print numerical statistics only, without administrative credentials."""
    try:
        client, database = connect("readonly")
        with client:
            stats = database.command("collStats", "raw_events")
            result = {
                key: stats.get(key)
                for key in (
                    "count",
                    "size",
                    "storageSize",
                    "totalIndexSize",
                    "indexSizes",
                    "nindexes",
                )
            }
        print(json.dumps(result, sort_keys=True))
    except Exception:
        print("ERROR: MongoDB statistics unavailable; check service and query credentials")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
