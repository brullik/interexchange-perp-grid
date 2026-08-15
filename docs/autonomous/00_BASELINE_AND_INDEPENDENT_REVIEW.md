# Baseline и решение независимой проверки

## Подтверждено

- PR №1 открыт, mergeable и остаётся Draft.
- Head равен `95a30f9821bae2c7515b07cba1f2ca51af2565de`.
- CI run `31887616649` завершён успешно четырьмя jobs:
  `verify`, `security`, `c4-critical-proof`, `docker-smoke`.
- C4 artifact `9247692764` привязан к exact head, содержит шесть требуемых
  файлов и сообщает 30/30 scenarios, 33 tests, 0 failures/errors/skips.
- Реализованы checksummed client IDs, long-running supervisor, qualification epochs,
  complete private snapshots, third-venue assessment, stable-FLAT loop, security lock
  и deployment scripts.

## Независимый C4 verdict

```text
C4.2: NO-GO
Blocker count: 1 confirmed P0
Blocker ID: C4.3-FLAT-RESULT
```

### Причина

`wait_for_stable_flat()` правильно возвращает:

```text
FlatBarrierResult(
    verified,
    report,
    consecutive_snapshots,
    event_watermark,
    timed_out,
)
```

Но оба caller-а:

- `LiveCanaryCoordinator._verify_stable_flat`;
- `LiveControlService._stable_report`

возвращают только `result.report`. Дальше решение принимается по
`report.flat_verified`. Если последний единичный snapshot пустой, но barrier не успел
выполнить quiet period/число последовательных snapshots и завершился timeout,
`result.verified=False`, а `report.flat_verified=True`. Такой результат нельзя
использовать для перехода в `FLAT`.

## Обязательное решение

Реализация C4.3 должна сохранить и проверять именно `FlatBarrierResult.verified`.
Ни один код не имеет права перейти в `FLAT` или вернуть `success=true`, если
`verified=false`, даже когда вложенный report имеет `flat_verified=true`.

## Другие выводы проверки

Текущая private reconciliation перечисляет все инструменты и вызывает REST по
каждому symbol. Это fail-closed, но не является допустимым hot path для множества
инструментов. До multi-route live оно заменяется native account-wide snapshot +
private WebSocket cache по решению из `02_LOCKED_PRODUCT_ARCHITECTURE.md`.

Репозиторий на момент проверки имеет visibility `public`. До добавления любых
operational evidence или секретов он должен стать Private.
