# C4.3 — обязательная узкая поправка stable-FLAT

## Запрещено

- переписывать coordinator, journal или reconciliation с нуля;
- уменьшать число snapshots;
- убирать watermark;
- считать одиночный REST snapshot terminal proof;
- возвращать success после timeout;
- ослаблять quarantine.

## Точное изменение API

### 1. Coordinator

Заменить:

```python
async def _verify_stable_flat(...) -> ReconciliationReport:
    result = await wait_for_stable_flat(...)
    return result.report
```

на:

```python
async def _verify_stable_flat(...) -> FlatBarrierResult:
    return await wait_for_stable_flat(...)
```

Каждый caller обязан использовать:

```python
barrier = await self._verify_stable_flat(action)
report = barrier.report
if not barrier.verified:
    # QUARANTINED / FLAT_BARRIER_TIMEOUT
```

### 2. Live control

`LiveControlService._stable_report()` также возвращает `FlatBarrierResult`.
`_flatten()` и `_mark_flat_if_needed()` не могут считать операцию успешной без
`barrier.verified`.

### 3. Domain/reason codes

Добавить стабильные reason codes:

```text
FLAT_BARRIER_TIMEOUT
FLAT_BARRIER_EVENT_RACE
FLAT_BARRIER_PRIVATE_STATE_UNKNOWN
```

`LiveControlResult` и `CanaryCycleResult` должны содержать:

```text
flat_barrier_verified: bool
flat_barrier_timed_out: bool
flat_barrier_snapshots: int
flat_barrier_watermark: int
```

### 4. State transition

Разрешённый переход в `FLAT`:

```text
only if barrier.verified == true
```

При `verified=false`:

```text
RECOVERING/CLOSING → QUARANTINED
```

Нельзя вызывать `_to_flat()`.

## Обязательные сценарии

| ID | Сценарий | Ожидание |
|---|---|---|
| SF-001 | Один flat snapshot, затем timeout | success=false, QUARANTINED |
| SF-002 | Два flat snapshots без завершённого quiet period | success=false |
| SF-003 | Flat, затем watermark увеличился | счётчик стабильности сброшен |
| SF-004 | Flat, затем late fill/position | recovery/flatten, не success |
| SF-005 | Два+ идентичных snapshots + quiet period + stable watermark | verified=true |
| SF-006 | Coordinator получает report.flat_verified=true, barrier.verified=false | переход в FLAT невозможен |
| SF-007 | LiveControl получает тот же конфликт | `success=false` |
| SF-008 | private state UNKNOWN во время barrier | timeout/quarantine |

## C4.3 proof

Создать artifact:

```text
c4-3-proof-<FULL_SHA>
```

Он включает exact node-ID manifest, JUnit, source/config/image hashes и явное
assertion:

```text
false_success_when_barrier_unverified = 0
production_submit_calls = 0
```
