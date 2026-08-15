from __future__ import annotations

import argparse
import importlib.metadata
import re
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

_PIN = re.compile(r"^(?P<name>[A-Za-z0-9_.-]+)==(?P<version>[^\s;]+)$")


def read_lock(path: Path) -> dict[str, str]:
    locked: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        matched = _PIN.fullmatch(line)
        if matched is None:
            raise ValueError(f"lock entry is not an exact pin: {line}")
        name = canonicalize_name(matched.group("name"))
        if name in locked:
            raise ValueError(f"duplicate lock package: {name}")
        locked[name] = matched.group("version")
    if not locked:
        raise ValueError("lock file is empty")
    return locked


def project_requirements(path: Path) -> tuple[Requirement, ...]:
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    project = payload["project"]
    values = list(project.get("dependencies", ()))
    for group in project.get("optional-dependencies", {}).values():
        values.extend(group)
    return tuple(Requirement(str(value)) for value in values)


def check_project(lock: dict[str, str], path: Path) -> None:
    for requirement in project_requirements(path):
        name = canonicalize_name(requirement.name)
        version = lock.get(name)
        if version is None:
            raise ValueError(f"project requirement missing from lock: {requirement.name}")
        if requirement.specifier and Version(version) not in requirement.specifier:
            raise ValueError(f"locked {name}=={version} violates {requirement.specifier}")


def check_installed(lock: dict[str, str]) -> None:
    installed = {
        canonicalize_name(distribution.metadata["Name"]): distribution.version
        for distribution in importlib.metadata.distributions()
        if distribution.metadata["Name"]
    }
    mismatches = tuple(
        f"{name}: locked={version} installed={installed.get(name)}"
        for name, version in lock.items()
        if installed.get(name) != version
    )
    if mismatches:
        raise ValueError("installed environment differs from lock:\n" + "\n".join(mismatches))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--pyproject", type=Path)
    parser.add_argument("--verify-installed", action="store_true")
    parser.add_argument("--required", nargs="*", default=())
    arguments = parser.parse_args()
    lock = read_lock(arguments.lock)
    if arguments.pyproject is not None:
        check_project(lock, arguments.pyproject)
    missing = tuple(
        name for value in arguments.required if (name := canonicalize_name(value)) not in lock
    )
    if missing:
        raise ValueError(f"required locked tools missing: {','.join(missing)}")
    if arguments.verify_installed:
        check_installed(lock)
    print(f"LOCK_OK packages={len(lock)} path={arguments.lock}")


if __name__ == "__main__":
    main()
