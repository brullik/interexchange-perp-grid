# Aggressive Symbiosis V1 — единственный fast-track план

Это единственный план реализации новой стратегии. Не создавай второй roadmap, PRD, ADR-каталог, requirements matrix, research backlog или параллельную систему статусов.

## Рабочий режим

- Базовая точка при подготовке пакета: `main` SHA `8ef3ad3dbf746917a5fa6cb46f366634ea5747f9`. Перед работой обязательно получить фактический `origin/main`; не откатывать более новые изменения.
- Одна ветка: `codex/aggressive-symbiosis-v1`.
- Один draft PR, который последовательно проходит все software-only этапы.
- Существующие execution, private adapters, journal, reconciliation, recovery, emergency, Telegram, qualification и Windows laptop workflows считаются рабочим baseline. Их не переписывать без падающего теста, доказывающего необходимость узкого изменения.
- Реализовать сначала один полный Wave 1 маршрут. Историю остальных маршрутов загружать on-demand после работающего вертикального среза.
- После каждого этапа запускать целевые тесты. Перед checkpoint-коммитом запускать `make verify` или существующий Windows-эквивалент.
- Не ждать длительную qualification внутри coding-сессии: реализовать и проверить runner на deterministic/short profile, затем использовать существующий durable Windows workflow. Длительное наблюдение не заменяет кодовые и fault-тесты.
- Обновлять только чекбоксы этого файла и верхний блок `STATUS.md`. Исторический журнал `STATUS.md` не удалять и не переписывать.

## A0 — зафиксировать baseline и границы изменения

- [x] Получить актуальный `origin/main`, зафиксировать полный SHA и проверить отсутствие незавершённого активного PR по этой цели.
- [x] Запустить текущий `make verify`/Windows-equivalent и сохранить честный baseline результата.
- [x] Подтвердить `mode=shadow`, `live_enabled=false`, `live_orders_allowed=false` и отсутствие production submit.
- [x] Составить краткую карту повторного использования существующих модулей; не создавать отдельный документ — записать одну строку решения в `STATUS.md`.
- [x] Создать/продолжить одну ветку и один draft PR.

**Выход:** текущая система зелёная либо каждое исходное падение явно отделено от новых изменений; live остаётся невозможным.

## A1 — канонические 1m reference-spread bars

- [x] Добавить минимальные typed models для закрытого source 1m OHLC и canonical 1m reference-spread OHLC.
- [x] Расширить существующую adapter/history boundary минимальной public OHLC capability; native override только при воспроизводимом capability gap.
- [x] Реализовать фиксированную canonical venue order и связь с двумя directed executable routes.
- [x] Реализовать точные формулы `O_A/O_B`, `H_A/L_B`, `L_A/H_B`, `C_A/C_B` с детерминированной precision/rounding policy.
- [x] Запретить forward-fill, несинхронные минуты, незакрытые бары, неоднозначные дубликаты и contract-version mismatch.
- [x] Реализовать агрегацию 1m spread bars в 5m/15m/1h/4h/1d; прямой расчёт из старших биржевых свечей физически не использовать.
- [x] Реализовать resumable, idempotent, rate-limit-aware on-demand history cache в существующем Parquet/DuckDB контуре.
- [x] Добавить одну CLI-команду/подкоманду, которая для заданной пары строит reference bars, выводит coverage/quality/hash и ничего не торгует.
- [x] Добавить deterministic fixtures и property tests для формул, пропусков, дубликатов, границ интервалов и restart-resume.

**Выход:** одинаковый набор source bars создаёт byte/hash-identical reference bars; неполный интервал не участвует в модели.

## A2 — historical reference model и агрессивная геометрия

- [x] Реализовать целевое окно 180d, live minimum 90d и shadow-only minimum 30d.
- [x] Рассчитать modal `S0` с точными tie-break rules и normal zone.
- [x] Отдельно рассчитать `H_plus`, `H_minus`, диапазоны и положительное/отрицательное направление.
- [x] Реализовать исторические convergence episodes, censoring, per-level convergence/adverse excursion и live gate 10 episodes + 70% within 24h.
- [x] Сохранить текущие 24h/7d/30d median/MAD/quantile statistics как current-regime и long-tail guard.
- [x] Реализовать regime-drift block и заморозку модели после первой части.
- [x] Рассчитать уровни 20/40/60/80/100%, веса 10/15/20/25/30%, reference stop +15% и effective stop с adaptive tail.
- [x] Версионировать и persist model identity: source-data hash, reference-bar hash, config hash, route identity, contract metadata version и code SHA.
- [x] Старые/неполные persisted calibration records мигрировать однозначно либо fail closed; не угадывать недостающие поля.

**Выход:** для обеих сторон пары модель и пять уровней воспроизводимы из истории и одинаковы после restart.

## A3 — полноценная persistent five-level state machine

- [ ] Хранить состояние каждого уровня: `ARMED`, `ENTRY_PENDING`, `OPEN`, `EXIT_PENDING`, `CLOSED_WAIT_REARM`, `DISABLED`.
- [ ] Выбирать `first_unfilled_crossed_level`, а не всегда `entry_levels_bps[0]`.
- [ ] Один уровень может быть заполнен ровно один раз до re-arm; шестая часть невозможна.
- [ ] При gap через несколько уровней открывать не более одной части за decision cycle, затем заново получать свежие L2 books, economics и risk.
- [ ] Каждой части принадлежит actual two-leg quantity/fills/fees/funding/target/stop/risk/model version.
- [ ] Реализовать reverse-grid exit глубоких частей и normal-zone exit первой части.
- [ ] Реализовать re-arm только после retreat минимум на 0.25 шага и повторного пересечения.
- [ ] После restart восстановить те же level states и запретить duplicate open/close.

**Выход:** deterministic replay демонстрирует уровни 1→5, частичные reverse exits, re-arm, повторную осцилляцию и полный stable-FLAT без превышения лимитов.

## A4 — hybrid entry, aggressive economics и sizing

- [ ] Один общий evaluator требует одновременно reference trigger и свежий executable L2/VWAP edge.
- [ ] Использовать profile `config/AGGRESSIVE_SYMBIOSIS_V1.yaml` как единственный источник новых числовых параметров.
- [ ] Установить normal cost multiplier 1.35 и minimum expected net profit 0.15 USDT; canary override 0.01 действует только в locked canary stage.
- [ ] Учитывать 50% прогнозируемого положительного funding, 100% неблагоприятного и 2x adverse funding stress.
- [ ] Запрещать вход, если convergence PnL без положительного funding неположителен.
- [ ] Сохранять actual private taker fees; unknown fee/funding/depth блокирует вход.
- [ ] Рассчитывать полный размер с весами частей так, чтобы modelled route loss <=4.50 USDT, hard projected <=5.00 USDT.
- [ ] Для портфеля использовать normal admission <=45 USDT и hard projected <=50 USDT.
- [ ] После lot/step rounding и каждого фактического fill пересчитывать риск; уменьшать/пропускать часть при нехватке residual budget.
- [ ] Подключить executable stop и hard projected-loss exit в replay, shadow и live supervisor с одинаковым приоритетом.
- [ ] Реализовать deterministic route score и tie-breakers из profile.

**Выход:** property/fault tests доказывают лимиты после каждого accepted action, реальную остановку по stop и отсутствие входа только по красивому, но неисполняемому reference spread.

## A5 — единый evaluator в replay, shadow и live

- [ ] Удалить/обойти упрощённые параллельные decision paths: один decision core, одна model identity и одни reason codes во всех режимах.
- [ ] Replay исполняет worst-case ordering, если внутри минуты невозможно доказать последовательность target/stop/level.
- [ ] Real-time shadow работает на живых public Wave 1 data, строит/обновляет on-demand history и ведёт пять simulated tranches.
- [ ] Live coordinator получает уже принятую immutable tranche intent и не повторяет стратегическую логику отдельно.
- [ ] Сохранить protected IOC, journal-before-submit, actual-fill reconciliation, third-venue hedge, emergency flatten и stable-FLAT.
- [ ] Проверить restart/process-kill в каждом активном level/action state.
- [ ] Добавить числовой decision breakdown и reason codes для reference, regime, economics, funding, risk, level, re-arm и exit.
- [ ] Обновить qualification evidence, включив все пять levels/weights/stops, historical/reference hashes и profile hash.

**Выход:** один и тот же event stream создаёт одинаковые decisions в replay и shadow; live принимает те же immutable intents, но не может быть включён тестами/конфигом.

## A6 — Windows-native laptop workflow

- [ ] Создать один wrapper `scripts/laptop-aggressive.ps1`, переиспользующий существующие onboarding, native manifest, qualification, pilot и S4U scripts.
- [ ] Поддержать режимы `verify`, `shadow`, `qualify`, `canary`, `pilot`, `status`, `stop` без второго orchestration framework.
- [ ] `verify` устанавливает/проверяет exact Python 3.12 environment и запускает полный Windows-equivalent verify без production credentials.
- [ ] `shadow` запускает live-public aggressive shadow на ноутбуке и не допускает private submit.
- [ ] `qualify` связывает exact code/config/profile/reference-data/runtime hashes; существующее 12h owner exception можно использовать только в его уже разрешённых границах, не сокращая дальше.
- [ ] `canary` переиспользует local DPAPI/S4U secrets, отдельное owner consent, Telegram challenge и один minimum-notional/one-tranche route с hard risk <=1 USDT.
- [ ] `pilot` после successful canary поддерживает один route, все пять tranches и route risk <=5 USDT; каждое stage promotion требует отдельного owner confirmation.
- [ ] Любой failure возвращает shadow/live=false, сохраняет evidence и запускает существующий recovery/stable-FLAT путь.
- [ ] После успешного laptop pilot и минимум 28,800 секунд post-FLAT service создать exact-bound `state/laptop-aggressive-acceptance.json`.
- [ ] Любая VPS/deploy команда нового профиля fail closed без accepted laptop artifact.

**Выход:** Codex может полностью проверить software/public-shadow без владельца; для secrets и real-money остаётся один точный owner action.

## A7 — software acceptance, review и merge

- [ ] Запустить все focused tests и полный `make verify`/Windows-equivalent.
- [ ] Подтвердить обязательные checks: `verify`, `security`, `c4-critical-proof`, `c4-3-proof`, `docker-smoke` либо их актуальные protected-main successors.
- [ ] Сформировать exact-head replay/fault/restart/laptop-shadow artifacts без production submit.
- [ ] Получить независимый review, исправить P0/P1/P2 и разрешить все material threads.
- [ ] Обновить верхний блок `STATUS.md`, не удаляя историю.
- [ ] Mark Ready и squash-merge единственный PR автоматически, только если head SHA неизменен и branch protection полностью зелёная.

**Выход:** aggressive software и Windows public-shadow готовы на `main`; это не live-money authorization.

## A8 — owner-operated laptop live ladder

Этот этап нельзя подменять fabricated evidence и нельзя выполнять без локальных restricted credentials и явного решения владельца.

- [ ] Выдать один owner action для локального DPAPI onboarding restricted trade-only/no-withdrawal credentials и его machine-readable validation.
- [ ] После явного owner consent выполнить minimum-notional canary на ноутбуке.
- [ ] Доказать filled open/close legs, exact fees/funding, reconciliation, stable-FLAT и post-FLAT service.
- [ ] При дефекте вернуть live=false, исправить его в той же scoped цели/одном follow-up PR, повторить software gates и только затем новый owner-confirmed canary.
- [ ] После успешного canary отдельным подтверждением выполнить laptop `pilot_a`: один route, до пяти уровней, hard route risk <=5 USDT.
- [ ] Создать и проверить `state/laptop-aggressive-acceptance.json` с `accepted=true` и exact hashes.

**Выход:** алгоритм реально работает на ноутбуке. До этого VPS запрещён.

## A9 — только подготовка последующего VPS handoff

- [ ] Подготовить минимальный export/check command, который принимает только accepted laptop artifact и exact merged release identity.
- [ ] Не выполнять VPS upload/deploy/qualification/live в этой цели.
- [ ] В финальном отчёте указать одну следующую цель для VPS без создания новой инфраструктуры или стратегии.

**Выход:** воспроизводимый handoff подготовлен, но ни один VPS не изменён.

## Разрешённые причины остановки

Codex останавливается только когда:

1. A0–A7 полностью завершены и остался owner action из A8; или
2. все A0–A9 завершены с честным accepted laptop live artifact; или
3. обнаружен настоящий внешний blocker, который невозможно устранить кодом, тестом, mock/replay, публичным API или уже доступными GitHub правами.

Перед остановкой завершить всю независимую работу. Не создавать owner action для обычного выбора реализации, долгого теста, CI, review, merge, документации или исправимого дефекта.
