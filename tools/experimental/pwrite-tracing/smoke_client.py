#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Write four small blocks after a user attaches a syscall tracer."""

import argparse
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output",
        type=Path,
        help="new output file; the command does not overwrite an existing file",
    )
    args = parser.parse_args()

    print("Target TGID: {}".format(os.getpid()), flush=True)
    input("Attach pwrite_smoke.bt to this TGID, then press Enter here: ")

    fd = os.open(
        str(args.output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
    )
    try:
        for index in range(4):
            payload = bytes([65 + index]) * 4096
            written = os.pwrite(fd, payload, index * len(payload))
            if written != len(payload):
                raise OSError(
                    "short pwrite: {} of {} bytes".format(
                        written, len(payload)
                    )
                )
        os.fdatasync(fd)
    finally:
        os.close(fd)

    print("Wrote 4 complete pwrite calls (16384 bytes).", flush=True)


if __name__ == "__main__":
    main()
