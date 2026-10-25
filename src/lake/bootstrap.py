"""Create the lake bucket with the admin key (`make s3-up`); the only use of that key.

The pipeline key can read, write and list objects in the lake bucket but cannot create buckets,
so the bucket is created here, once, before any export.
"""

import argparse
import sys
from collections.abc import Sequence

from src.config import ConfigError, load_s3_settings
from src.lake.s3 import S3_ERRORS, create_bucket, create_client


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Create the lake bucket (admin key).")
    parser.parse_args(argv)
    try:
        settings = load_s3_settings("admin")
        if settings.aws_mode:
            raise ConfigError("On AWS the bucket is created by Terraform (infra/aws), not here.")
        created = create_bucket(create_client(settings), settings.bucket, settings.region)
    except S3_ERRORS as exc:
        print(f"ERROR: S3 request failed: {exc}", file=sys.stderr)
        return 1
    except ConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    state = "created" if created else "already exists"
    print(f"bucket {settings.bucket}: {state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
