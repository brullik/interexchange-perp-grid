from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path

from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.qualification import (
    LAPTOP_OWNER_EXCEPTION_CONFIRMATION,
    LAPTOP_OWNER_EXCEPTION_ENV,
    QualificationProgress,
    build_qualification_progress,
    laptop_owner_exception_authorized,
    laptop_owner_exception_policy,
    qualification_policy_from_settings,
)
from interexchange_perp_grid.state import initialise_state


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded qualification status worker")
    parser.add_argument("--epoch-id", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--laptop-owner-exception-12h", action="store_true")
    arguments = parser.parse_args()
    settings = load_settings(arguments.config)
    if arguments.laptop_owner_exception_12h:
        if not laptop_owner_exception_authorized():
            parser.error(
                "Windows laptop exception requires "
                f"{LAPTOP_OWNER_EXCEPTION_ENV}={LAPTOP_OWNER_EXCEPTION_CONFIRMATION}"
            )
        policy = laptop_owner_exception_policy(settings)
    else:
        policy = qualification_policy_from_settings(settings)

    async def read() -> QualificationProgress:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        return await build_qualification_progress(
            state_path,
            Path(settings.storage.parquet_dir),
            arguments.epoch_id,
            policy,
        )

    try:
        progress = asyncio.run(read())
    except KeyError:
        raise SystemExit(4) from None
    print(json.dumps(asdict(progress), default=str, sort_keys=True))


if __name__ == "__main__":
    main()
