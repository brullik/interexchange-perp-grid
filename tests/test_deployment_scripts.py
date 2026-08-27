from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ENTRY = REPO_ROOT / "scripts" / "shadow-deploy.sh"
UPGRADE_ENTRY = REPO_ROOT / "scripts" / "shadow-upgrade.sh"
DEPLOY = REPO_ROOT / "scripts" / "shadow-deploy-mechanics.sh"
UPGRADE = REPO_ROOT / "scripts" / "shadow-upgrade-mechanics.sh"
BOOTSTRAP = REPO_ROOT / "scripts" / "bootstrap-ubuntu.sh"
IPEGCTL = REPO_ROOT / "scripts" / "ipegctl"
OLD_IMAGE = f"ghcr.io/example/app@sha256:{'1' * 64}"
NEW_IMAGE = f"ghcr.io/example/app@sha256:{'2' * 64}"
OLD_SHA = "a" * 40
NEW_SHA = "b" * 40


def _bash() -> str:
    executable = shutil.which("bash")
    if executable is None:
        pytest.skip("deployment scripts require bash")
    probe = subprocess.run(
        [executable, "-c", "exit 0"],
        check=False,
        capture_output=True,
    )
    if probe.returncode != 0:
        pytest.skip("deployment scripts require a usable bash runtime")
    return executable


def _fake_environment(tmp_path: Path) -> tuple[dict[str, str], Path, Path]:
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    docker_log = tmp_path / "docker.log"
    docker = binary_dir / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s|%s\\n' "${IPEG_RELEASE_SHA:-none}" "$*" >>"$FAKE_DOCKER_LOG"
if [[ "$*" == "compose ps --status running --services" ]]; then
  [[ "${FAKE_APP_RUNNING:-0}" == 1 ]] && printf 'app\\n'
  exit 0
fi
if [[ "$*" == image\\ inspect*org.opencontainers.image.revision* ]]; then
  printf '%s\\n' "${FAKE_IMAGE_REVISION:-${IPEG_RELEASE_SHA:-}}"
  exit 0
fi
if [[ "$*" == *"deployment-upgrade-gate"*"--action arm"* ]] \
  && [[ "${FAKE_UPGRADE_GATE_ARM_FAIL:-0}" == 1 ]]; then
  exit 6
fi
if [[ "$*" == *"deployment-upgrade-gate"*"--action release"* ]] \
  && [[ "${FAKE_UPGRADE_GATE_RELEASE_FAIL:-0}" == 1 ]]; then
  exit 7
fi
if [[ "$*" == *"backup-state"* ]] && [[ "${FAKE_BACKUP_FAIL:-0}" == 1 ]]; then
  exit 4
fi
if [[ "$*" == *"interexchange-grid health"* ]] \
  && { [[ "${IPEG_RELEASE_SHA:-}" == "${FAKE_FAIL_SHA:-never}" ]] \
    || [[ "${IPEG_RELEASE_SHA:-}" == "${FAKE_FAIL_SECOND_SHA:-never}" ]]; }; then
  exit 17
fi
exit 0
""",
        encoding="utf-8",
    )
    docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
    git = binary_dir / "git"
    git.write_text(
        """#!/usr/bin/env bash
if [[ "$*" == "rev-parse --is-inside-work-tree" ]]; then
  printf 'true\\n'
  exit 0
fi
if [[ "$*" == "ls-files --error-unmatch -- .env" ]]; then
  exit 1
fi
exit 2
""",
        encoding="utf-8",
    )
    git.chmod(git.stat().st_mode | stat.S_IXUSR)
    secrets = tmp_path / ".env"
    secrets.write_text("IPEG_LIVE_ENABLED=false\n", encoding="utf-8")
    secrets.chmod(0o600)
    state_path = tmp_path / ".ipeg-deployment-state"
    environment = {
        **os.environ,
        "PATH": f"{binary_dir}{os.pathsep}{os.environ['PATH']}",
        "FAKE_DOCKER_LOG": str(docker_log),
        "IPEG_DEPLOYMENT_STATE_PATH": str(state_path),
    }
    return environment, state_path, docker_log


def _run(
    script: Path,
    *arguments: str,
    cwd: Path,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [_bash(), str(script), *arguments],
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_deploy_requires_external_secrets_with_mode_0600(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    (tmp_path / ".env").chmod(0o644)

    result = _run(DEPLOY, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 3
    assert "mode 0600" in result.stderr
    assert not state_path.exists()
    assert not docker_log.exists()


def test_deploy_is_physically_blocked_without_exact_laptop_acceptance(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    result = _run(DEPLOY_ENTRY, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 9
    assert "VPS bootstrap/deploy/upgrade is disabled" in result.stderr
    assert not state_path.exists()
    assert not docker_log.exists()


def test_upgrade_is_physically_blocked_before_docker_mutation(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)

    result = _run(UPGRADE_ENTRY, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 9
    assert "VPS bootstrap/deploy/upgrade is disabled" in result.stderr
    assert not state_path.exists()
    assert not docker_log.exists()


def test_bootstrap_rejects_destdir_that_resolves_to_physical_root(tmp_path: Path) -> None:
    environment, _, _ = _fake_environment(tmp_path)
    environment["DESTDIR"] = "/.."
    os_release = tmp_path / "os-release"
    os_release.write_text('ID=ubuntu\nVERSION_ID="24.04"\n', encoding="utf-8")
    environment["IPEG_OS_RELEASE_PATH"] = str(os_release)

    result = _run(BOOTSTRAP, cwd=tmp_path, environment=environment)

    assert result.returncode == 2
    assert "must not resolve" in result.stderr


def test_deploy_is_idempotent_and_persists_only_healthy_exact_identity(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)

    first = _run(DEPLOY, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)
    second = _run(DEPLOY, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert first.returncode == second.returncode == 0
    assert state_path.read_text(encoding="utf-8") == (
        f"image_ref={NEW_IMAGE}\nrelease_sha={NEW_SHA}\n"
    )
    log = docker_log.read_text(encoding="utf-8")
    assert log.count("compose up --detach --no-build --wait --wait-timeout 180 app") == 2
    assert log.count("interexchange-grid deployment-identity") == 2


def test_deploy_rejects_digest_whose_revision_label_does_not_match_sha(
    tmp_path: Path,
) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    environment["FAKE_IMAGE_REVISION"] = OLD_SHA

    result = _run(DEPLOY, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 4
    assert "revision label" in result.stderr
    assert not state_path.exists()
    assert "compose up" not in docker_log.read_text(encoding="utf-8")


def test_failed_upgrade_restores_backup_and_previous_digest(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_APP_RUNNING"] = "1"
    environment["FAKE_FAIL_SHA"] = NEW_SHA

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 17
    assert "automatic rollback completed" in result.stderr
    assert state_path.read_text(encoding="utf-8") == (
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n"
    )
    log = docker_log.read_text(encoding="utf-8")
    assert "backup-state" in log
    assert "compose stop app" in log
    assert "restore-state" in log
    assert f"{NEW_SHA}|compose run --rm --no-deps app interexchange-grid restore-state" in log
    assert f"{NEW_SHA}|compose up" in log
    assert f"{OLD_SHA}|compose up" in log
    assert log.count("deployment-upgrade-gate --config /app/config/defaults.yaml") == 3
    assert log.index("compose pause app") < log.index("--action arm")
    assert log.index("backup-state") < log.index("--action arm")
    assert log.index("--action arm") < log.index("compose kill app")
    assert log.rindex("--action release") > log.rindex(f"{OLD_SHA}|compose up")
    assert f"{OLD_SHA}|compose run --rm --no-deps app interexchange-grid backup-state" in log


def test_upgrade_refuses_to_stop_app_when_durable_live_actions_are_active(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_APP_RUNNING"] = "1"
    environment["FAKE_UPGRADE_GATE_ARM_FAIL"] = "1"

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 6
    assert "aborted before deployment" in result.stderr
    log = docker_log.read_text(encoding="utf-8")
    assert "--action arm" in log
    assert "compose pause app" in log
    assert "compose unpause app" in log
    assert "compose kill app" not in log
    assert log.index("backup-state") < log.index("--action arm")
    assert f"{NEW_SHA}|compose up" not in log


def test_successful_upgrade_releases_entry_freeze_only_after_exact_health(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_APP_RUNNING"] = "1"

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 0, result.stderr
    log = docker_log.read_text(encoding="utf-8")
    assert log.index("compose pause app") < log.index("--action arm")
    assert log.index("backup-state") < log.index("--action arm")
    assert log.index("--action arm") < log.index("compose kill app")
    assert "compose run --rm --no-deps app interexchange-grid deployment-upgrade-gate" in log
    assert f"{NEW_SHA}|image inspect {NEW_IMAGE}" in log
    assert log.index("interexchange-grid deployment-identity") < log.rindex("--action release")
    assert state_path.read_text(encoding="utf-8") == (
        f"image_ref={NEW_IMAGE}\nrelease_sha={NEW_SHA}\n"
    )


def test_stopped_previous_service_is_backed_up_and_gated_before_upgrade(tmp_path: Path) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 0, result.stderr
    log = docker_log.read_text(encoding="utf-8")
    assert f"{OLD_SHA}|compose run --rm --no-deps app interexchange-grid backup-state" in log
    assert log.index("backup-state") < log.index("--action arm")
    assert "compose pause app" not in log
    assert "compose kill app" not in log
    assert log.index("--action arm") < log.index(f"{NEW_SHA}|compose up")


def test_stopped_service_with_active_actions_restarts_exact_previous_recovery(
    tmp_path: Path,
) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_UPGRADE_GATE_ARM_FAIL"] = "1"

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 6
    assert "old service resumed for risk reduction" in result.stderr
    log = docker_log.read_text(encoding="utf-8")
    assert log.index("backup-state") < log.index("--action arm")
    assert f"{OLD_SHA}|compose up --detach --no-build --wait --wait-timeout 180 app" in log
    assert f"{NEW_SHA}|compose up" not in log


def test_concurrent_upgrade_is_rejected_before_docker_mutation(tmp_path: Path) -> None:
    fcntl = pytest.importorskip("fcntl")
    environment, state_path, docker_log = _fake_environment(tmp_path)
    lock_path = Path(f"{state_path}.upgrade.lock")
    with lock_path.open("w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 8
    assert "already in progress" in result.stderr
    assert not docker_log.exists()


def test_upgrade_fails_closed_when_healthy_service_cannot_release_entry_freeze(
    tmp_path: Path,
) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_APP_RUNNING"] = "1"
    environment["FAKE_UPGRADE_GATE_RELEASE_FAIL"] = "1"

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 7
    assert "could not release the upgrade entry freeze" in result.stderr
    log = docker_log.read_text(encoding="utf-8")
    assert "restore-state" in log
    assert f"{OLD_SHA}|compose up --detach --no-build --wait --wait-timeout 180 app" in log
    assert "--action release" in log


def test_failed_rollback_keeps_legacy_gate_armed_and_never_releases_entry(
    tmp_path: Path,
) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_APP_RUNNING"] = "1"
    environment["FAKE_FAIL_SHA"] = NEW_SHA
    environment["FAKE_FAIL_SECOND_SHA"] = OLD_SHA

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 5
    assert "rollback failed" in result.stderr
    log = docker_log.read_text(encoding="utf-8")
    old_start = log.rindex(f"{OLD_SHA}|compose up")
    assert log.rindex("--action arm") < old_start
    assert "--action release" not in log[old_start:]


def test_backup_failure_releases_gate_while_stopped_then_restarts_exact_old_image(
    tmp_path: Path,
) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_APP_RUNNING"] = "1"
    environment["FAKE_BACKUP_FAIL"] = "1"

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 4
    assert "paused-state backup failed" in result.stderr
    log = docker_log.read_text(encoding="utf-8")
    assert "compose pause app" in log
    assert "compose unpause app" in log
    assert "compose kill app" not in log
    assert "deployment-upgrade-gate" not in log


def test_stopped_service_backup_failure_restarts_exact_previous_recovery(
    tmp_path: Path,
) -> None:
    environment, state_path, docker_log = _fake_environment(tmp_path)
    state_path.write_text(
        f"image_ref={OLD_IMAGE}\nrelease_sha={OLD_SHA}\n",
        encoding="utf-8",
    )
    environment["FAKE_BACKUP_FAIL"] = "1"

    result = _run(UPGRADE, NEW_IMAGE, NEW_SHA, cwd=tmp_path, environment=environment)

    assert result.returncode == 4
    log = docker_log.read_text(encoding="utf-8")
    assert "backup-state" in log
    assert f"{OLD_SHA}|compose up --detach --no-build --wait --wait-timeout 180 app" in log
    assert f"{NEW_SHA}|compose up" not in log


def test_bootstrap_stages_exact_ubuntu_systemd_control_plane(tmp_path: Path) -> None:
    os_release = tmp_path / "os-release"
    os_release.write_text('ID=ubuntu\nVERSION_ID="24.04"\n', encoding="utf-8")
    destination = tmp_path / "root"
    environment = {
        **os.environ,
        "DESTDIR": str(destination),
        "IPEG_OS_RELEASE_PATH": str(os_release),
        "IPEG_SKIP_PACKAGE_INSTALL": "true",
    }

    result = _run(BOOTSTRAP, cwd=REPO_ROOT, environment=environment)

    assert result.returncode == 0, result.stderr
    assert (destination / "usr/local/sbin/ipegctl").is_file()
    assert (destination / "usr/local/sbin/ipeg-bootstrap").is_file()
    assert (destination / "etc/systemd/system/ipeg.service").is_file()
    assert (destination / "opt/ipeg/docker-compose.yml").is_file()
    unit = (destination / "etc/systemd/system/ipeg.service").read_text(encoding="utf-8")
    assert "Restart=always" in unit
    assert "IPEG_ENV_FILE=/etc/ipeg/ipeg.env" in unit


def test_ipegctl_exposes_locked_commands_and_refuses_canary_without_consent(
    tmp_path: Path,
) -> None:
    syntax = subprocess.run(
        [_bash(), "-n", str(IPEGCTL)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert syntax.returncode == 0, syntax.stderr

    result = _run(IPEGCTL, "canary-arm", cwd=tmp_path, environment=dict(os.environ))

    assert result.returncode == 5
    assert "LIVE_CANARY_CONSENT" in result.stderr
    text = IPEGCTL.read_text(encoding="utf-8")
    for command in (
        "bootstrap",
        "deploy",
        "doctor",
        "status",
        "start-shadow",
        "qualification-start",
        "qualification-status",
        "qualification-finalize",
        "owner-onboard",
        "canary-arm",
        "canary-status",
        "emergency-flatten",
        "update",
        "rollback",
        "backup",
        "logs",
    ):
        assert command in text
    assert 'qualification_route="BTC:bybit>okx"' in text
    assert "IPEG_OWNER_ONBOARDING_CONFIRMED=true" in text
    assert "withdrawal, transfer, wallet, address-book" in text
    assert "VPS IP allowlist" in text
    assert "dedicated subaccounts are funded" in text
    assert "Qualification route (BASE:long>short)" not in text


def test_owner_onboard_is_atomic_locked_and_mode_0600(tmp_path: Path) -> None:
    script = shutil.which("script")
    sudo = shutil.which("sudo")
    if script is None or sudo is None:
        pytest.skip("owner onboarding integration requires script and sudo")
    sudo_probe = subprocess.run([sudo, "-n", "true"], check=False, capture_output=True, timeout=5)
    if sudo_probe.returncode != 0:
        pytest.skip("owner onboarding integration requires passwordless sudo")

    env_file = tmp_path / "ipeg.env"
    command = " ".join(
        (
            shlex.quote(sudo),
            "env",
            f"IPEG_ENV_FILE={shlex.quote(str(env_file))}",
            shlex.quote(str(IPEGCTL)),
            "owner-onboard",
        )
    )

    declined = subprocess.run(
        [script, "--quiet", "--return", "--command", command, "/dev/null"],
        input="NO\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert declined.returncode == 3
    assert not env_file.exists()

    answers = [
        "CONFIRM",
        "CONFIRM",
        "CONFIRM",
        "CONFIRM",
        "42",
        "telegram-token",
        "binance-key",
        "binance-secret",
        "",
        "bybit-key",
        "bybit-secret",
        "",
        "okx-key",
        "okx-secret",
        "okx-passphrase",
    ]
    accepted = subprocess.run(
        [script, "--quiet", "--return", "--command", command, "/dev/null"],
        input="\n".join(answers) + "\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert accepted.returncode == 0, accepted.stderr
    contents_result = subprocess.run(
        [sudo, "cat", str(env_file)],
        text=True,
        capture_output=True,
        check=False,
        timeout=5,
    )
    assert contents_result.returncode == 0, contents_result.stderr
    contents = contents_result.stdout
    assert "IPEG_MODE=shadow" in contents
    assert "IPEG_LIVE_ENABLED=false" in contents
    assert "IPEG_OWNER_ONBOARDING_CONFIRMED=true" in contents
    assert "IPEG_QUALIFICATION_ROUTE=BTC:bybit>okx" in contents
    mode_result = subprocess.run(
        [sudo, "stat", "--format=%a", str(env_file)],
        text=True,
        capture_output=True,
        check=False,
        timeout=5,
    )
    assert mode_result.returncode == 0, mode_result.stderr
    assert mode_result.stdout.strip() == "600"
