# Стартовая цель для Codex — Aggressive Symbiosis V1

Отправьте Codex весь текст ниже одной задачей.

```text
/goal

Репозиторий: brullik/interexchange-perp-grid
Локальный первый runtime: Windows-ноутбук владельца
Целевая ветка: codex/aggressive-symbiosis-v1

ЕДИНАЯ ЦЕЛЬ

Максимально быстро и автономно реализовать Aggressive Symbiosis V1: детерминированный historical 1m OHLC reference-spread pipeline + текущая robust/adaptive regime model + фактически исполнимый L2/VWAP spread + persistent five-level back-loaded grid + существующий защищённый paired execution/recovery. Сначала полностью проверить и запустить алгоритм на ноутбуке. Реальный live на ноутбуке должен быть технически готов и запускаться только после локальных restricted credentials и отдельного явного owner confirmation. Любой VPS upload/deploy/live запрещён до accepted laptop artifact.

ПАКЕТ-КОНТРАКТ

Найди приложенный пакет и установи в корень репозитория с сохранением указанных путей:

- repo_files/AGENTS.md -> AGENTS.md
- repo_files/GOAL.md -> GOAL.md
- repo_files/FAST_TRACK_PLAN.md -> FAST_TRACK_PLAN.md
- repo_files/ACCEPTANCE.md -> ACCEPTANCE.md
- repo_files/config/AGGRESSIVE_SYMBIOSIS_V1.yaml -> config/AGGRESSIVE_SYMBIOSIS_V1.yaml
- repo_files/CODEX_START_PROMPT_RU.md -> CODEX_START_PROMPT_RU.md
- repo_files/CODEX_RESUME_PROMPT_RU.md -> CODEX_RESUME_PROMPT_RU.md

Не заменяй и не сокращай STATUS.md: сохрани всю историю и обновляй только его верхний current-state блок и краткие новые checkpoint/evidence записи.

Сначала:

1. Получи фактический origin/main и полный SHA. Пакет подготовлен относительно main 8ef3ad3dbf746917a5fa6cb46f366634ea5747f9, но более новые изменения имеют приоритет и не должны быть потеряны.
2. Проверь отсутствие другого активного PR по этой же цели. Если ветка/PR codex/aggressive-symbiosis-v1 уже существуют, безопасно продолжи их; иначе создай одну ветку и один draft PR.
3. Один раз полностью прочитай только AGENTS.md, GOAL.md, FAST_TRACK_PLAN.md, ACCEPTANCE.md, config/AGGRESSIVE_SYMBIOSIS_V1.yaml и актуальный верх STATUS.md.
4. Запусти текущий baseline verify и зафиксируй исходное состояние. Default/live должны оставаться shadow/false, production submit = 0.
5. Немедленно начинай реализацию с первого незавершённого checkpoint. Не останавливайся на плане, аудите, документации или каркасе.

ЖЁСТКАЯ ГРАНИЦА SCOPE

Не переписывай уже работающие:

- ExchangeAdapter и существующие venue transports;
- broad BBO / candidate L2;
- protected aggressive IOC execution;
- private streams, journal, idempotent client IDs;
- actual-fill reconciliation;
- partial-fill correction, third-venue hedge, emergency flatten;
- atomic risk ownership, restart recovery и stable-FLAT;
- Telegram owner challenge;
- Windows native manifest, DPAPI/S4U onboarding, qualification и laptop pilot foundation.

Расширяй их только узко, если новый executable acceptance test доказывает необходимость. Основной scope: reference 1m bars/history, historical model, five-level state machine, economics/risk/exit wiring, replay-shadow-live parity, laptop wrapper/evidence.

ОБЯЗАТЕЛЬНЫЙ АЛГОРИТМ

Реализуй точно GOAL.md и config/AGGRESSIVE_SYMBIOSIS_V1.yaml. Критические свойства:

- закрытые синхронные 1m OHLC;
- spread O=O_A/O_B, H=H_A/L_B, L=L_A/H_B, C=C_A/C_B;
- старшие интервалы только из готовых 1m spread bars;
- никаких forward-fill и прямых spread 1h из exchange 1h;
- canonical reference pair и два directed executable routes;
- S0 = детерминированная mode, separate H+/H-, historical episodes;
- current 24h/7d/30d adaptive model остаётся regime/long-tail guard;
- levels 20/40/60/80/100%; weights 10/15/20/25/30%; stop buffer 15%;
- first_unfilled_crossed_level, один tranche за cycle, fresh L2/risk между catch-up частями;
- reverse-grid close и re-arm после retreat 0.25 step;
- normal cost multiplier 1.35; minimum net 0.15 USDT; canary-only 0.01;
- 50% favorable funding credit, 100% adverse, 2x adverse stress;
- route modelled risk <=4.50 USDT, hard projected <=5.00;
- portfolio normal admission <=45 USDT, hard projected <=50.00;
- hard stop реально закрывает в replay, shadow и live supervisor;
- один shared evaluator для replay/shadow/live;
- live всегда использует свежий executable L2/VWAP и actual fills.

МЕТОД МАКСИМАЛЬНО БЫСТРОЙ РЕАЛИЗАЦИИ

- Сделай один полный Wave 1 vertical slice прежде широкого universe rollout.
- Историю загружай on-demand для кандидатов/открытых routes; не скачивай весь рынок заранее.
- Используй существующие Python 3.12, asyncio, Decimal, SQLite WAL, Parquet и DuckDB.
- Не добавляй web UI, microservices, Redis, Kafka, Celery, Kubernetes, ML, новый framework или вторую БД.
- Не создавай новый PRD, roadmap, ADR, audit report, requirements matrix, research backlog или status-файлы.
- Не делай массовый рефакторинг ради стиля.
- Не меняй exchange transport без measured capability gap.
- После каждого coherent slice запускай focused tests; перед каждым checkpoint commit — полный make verify/Windows equivalent.
- При долгом qualification не жди пассивно: используй существующий durable Windows Task Scheduler/S4U workflow и продолжай все независимые задачи. Не сокращай qualification ниже уже отдельно разрешённой 12h laptop-only policy.
- Любой найденный дефект исправляй сразу в этой цели, добавляя regression test. Не проси владельца выбирать техническое решение.

АВТОНОМНОСТЬ GITHUB

В рамках одной ветки/PR самостоятельно:

- меняй код, конфигурацию, тесты и необходимые существующие документы;
- запускай/перезапускай CI;
- исправляй failures;
- запрашивай независимый review после exact-head green;
- устраняй P0/P1/P2 и material review threads;
- mark Ready;
- squash-merge PR, только если все required checks green, head SHA не изменился, scope точный и unresolved material threads отсутствуют.

Не создавай несколько PR для последовательных checkpoint. Допустим один узкий follow-up PR только если реальный laptop canary позже обнаружит дефект уже после software merge.

LAPTOP-FIRST DELIVERY

Создай один scripts/laptop-aggressive.ps1, который композиционно использует существующие laptop scripts и поддерживает:

- verify;
- shadow;
- qualify;
- canary;
- pilot;
- status;
- stop.

Software/public этапы выполни без production credentials. На ноутбуке Docker не обязателен. Все CI/replay/shadow evidence должны иметь production_submit_calls=0.

После software merge выдай ровно один owner action только для действий, которые физически требуют владельца:

1. локально ввести restricted trade-only/no-withdrawal credentials в существующий DPAPI/S4U профиль, не показывая их Codex/чату/GitHub;
2. отдельно подтвердить minimum-notional live canary;
3. после successful canary отдельно подтвердить pilot_a: один route, до пяти tranches, hard route risk <=5 USDT.

До этого live физически выключен. Не запрашивай API keys, Telegram token, unlock secret или секретные значения в сообщении.

Успешный laptop pilot обязан закончиться exchange-verified stable-FLAT, zero active action, честными fill/fee/funding/reconciliation evidence и не менее 28,800 секунд post-FLAT service. Только после этого создай локальный ignored state/laptop-aggressive-acceptance.json с accepted=true и exact hashes.

VPS ЗАПРЕЩЁН

Не подключайся к VPS, не загружай туда файлы, не deploy, не переносись secrets и не запускай qualification/live. Можно подготовить только fail-closed handoff command, который отказывает без verified accepted laptop artifact и exact merged release identity.

НЕИЗМЕНЯЕМЫЕ ОГРАНИЧЕНИЯ

- paired long/short only;
- linear USDT perpetual only;
- reference capital 500 USDT;
- max hard route projected loss 5 USDT;
- max hard portfolio projected loss 50 USDT;
- max 10 routes, one route per base, five tranches;
- cross bot-dedicated accounts/subaccounts;
- free margin >=20%; initial effective leverage <=3x;
- hard hold <=24h;
- no withdrawal/transfer/wallet/address-book/API-key management;
- protected taker execution with cap; unbounded market emergency-only;
- unknown/stale/incomplete/unreconciled state fails closed;
- no invented profitability, fills, qualification, latency or live evidence;
- realised loss and profit are never guaranteed.

ОСТАНОВКА

Не останавливайся из-за объёма задачи, CI, review, исправимого дефекта, недоступности одной биржи, отсутствия долгого результата в текущей сессии или обычного технического выбора.

Остановка допустима только когда:

A. A0–A7 из FAST_TRACK_PLAN.md завершены, software PR merged, Windows public-shadow accepted, и остался один точный owner action для локальных credentials/real-money consent; либо
B. laptop live ladder честно завершён и state/laptop-aggressive-acceptance.json accepted; либо
C. остался настоящий внешний blocker, который нельзя устранить кодом, тестом, mock/replay, public API, CI или уже доступными GitHub правами.

ФИНАЛЬНЫЙ ОТЧЁТ

Дай только проверяемое:

- merged/current SHA и PR;
- какие checkpoint/acceptance IDs завершены;
- точные команды и результаты tests/checks;
- ссылки/пути exact-head artifacts;
- production_submit_calls;
- laptop shadow/qualification/canary/pilot/stable-FLAT status без преувеличений;
- один remaining owner action, если он действительно остался;
- подтверждение, что VPS не изменялся.
```
