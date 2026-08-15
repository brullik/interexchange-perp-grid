# Окончательная стартовая цель для Codex

В Codex сначала включите быстрый режим отдельной командой, если он доступен в вашем интерфейсе:

```text
/fast on
```

Затем отправьте всё содержимое блока ниже одной задачей.

```text
/goal

Репозиторий: brullik/interexchange-perp-grid

ЕДИНАЯ ЦЕЛЬ

Как можно быстрее довести репозиторий до реально работающего вертикального продукта:

1. Product Ready — автономный shadow-продукт на живых публичных данных Binance USD-M,
   Bybit и OKX, который обнаруживает общие linear USDT perpetuals, рассчитывает
   исполнимый VWAP-спред, funding, комиссии и полный стресс-риск, записывает историю,
   калибрует адаптивную сетку, проводит парные сделки в детерминированном симуляторе
   и real-time shadow, восстанавливается после перезапуска и управляется через Telegram.

2. Live Canary Ready — безопасный private execution для Bybit и OKX с Binance USD-M
   как первым резервом, полностью проверенный без production-секретов и готовый к одному
   минимальному canary после отдельного owner action.

Сначала полностью прочитай только:

- AGENTS.md
- GOAL.md
- FAST_TRACK_PLAN.md
- ACCEPTANCE.md
- STATUS.md

После одного чтения немедленно начинай реализацию. Не создавай новый PRD, новый план,
requirements matrix, ADR-каталог или десятки документов. Существующие пять файлов —
единственный контракт и план. Обновляй только STATUS.md и чекбоксы FAST_TRACK_PLAN.md.

РАБОЧИЙ РЕЖИМ

- Создай ветку codex/fast-track-mvp.
- Веди один draft PR и последовательно доводи в нём C0 → C1 → C2 → C3 → C4.
- Не останавливайся после написания плана или каркаса.
- После каждого checkpoint запускай make verify, фиксируй evidence и продолжай дальше.
- Не жди долгую shadow-квалификацию в сессии: реализуй runner и evidence, проверь его на
  коротком/synthetic профиле и продолжай независимую работу.
- Не спрашивай владельца о технических решениях, которые можно принять обратимо.
  Выбирай самый простой вариант, соответствующий GOAL.md, и одной строкой записывай
  решение в STATUS.md.
- Если один exchange временно недоступен или его capability не подтверждена, переведи
  его в quarantine и продолжай с остальными. Не блокируй весь продукт.
- Используй subagents только для независимого чтения официальной API-документации,
  генерации тестов и отдельного review. Не разрешай двум агентам менять один модуль.

АРХИТЕКТУРНОЕ УСКОРЕНИЕ

- Python 3.12 + asyncio, один modular monolith на одном VPS.
- CCXT Pro как первоначальный transport за собственным ExchangeAdapter.
- Не пиши семь native connectors с нуля.
- Native venue override добавляй только при подтверждённом capability gap или измеренном
  дефекте CCXT Pro.
- SQLite WAL для транзакционного состояния.
- Parquet + DuckDB для истории и replay.
- Никаких Kafka, Redis, Celery, Kubernetes, microservices или web UI в MVP.
- Сначала Wave 1: Binance USD-M, Bybit, OKX.
- Затем canary-ready private path: Bybit + OKX, Binance USD-M alternate.
- Bitget/KuCoin, затем MEXC/BingX — только после готового вертикального среза.

НЕИЗМЕНЯЕМЫЕ ОГРАНИЧЕНИЯ

- Только paired long/short и linear USDT perpetuals.
- Reference capital 500 USDT.
- Projected stressed loss <= 5 USDT на route и <= 50 USDT по портфелю.
- Не более 10 обычных routes, одного route на base asset и пяти tranches.
- Cross margin только в выделенном аккаунте/субаккаунте бота.
- Не менее 20% свободной локальной маржи после stress.
- Effective leverage первого live <= 3x; sizing никогда не зависит от max leverage биржи.
- Динамическое удержание, абсолютный максимум 24 часа первого live-этапа.
- Обычное исполнение — protected aggressive taker с price/slippage cap.
- Unbounded market разрешён только для emergency hedge/close/liquidation prevention.
- Funding, четыре комиссии, market impact, latency, partial fill и forced-exit reserve
  обязательны в economics/risk.
- Default cost multiplier 2.0, minimum profit калибруется replay/shadow.
- API withdrawal/transfer отсутствуют полностью.
- Live невозможно включить одним config flag.
- Любая неопределённость данных, capability, order state или reconciliation — fail closed.
- Ноль сделок является корректным результатом при отсутствии положительной экономики.
- Никаких реальных ключей, токенов или выдуманного evidence.

ПЕРВЫЙ ПРАКТИЧЕСКИЙ РЕЗУЛЬТАТ

Не делай документационный milestone. Сразу обеспечь работающую сквозную демонстрацию:

synthetic/replay market events
→ normalised books
→ directed executable spread
→ adaptive signal
→ stressed economics
→ atomic risk reservation
→ paired simulated fills
→ tranche ledger
→ partial close/full close
→ persisted state
→ restart/reconciliation
→ CLI/Telegram visibility.

Затем подключи живые public Wave 1 streams к тому же контуру и доведи его до Product Ready.
После этого реализуй C4 private/canary-ready path без production credentials.

ОСТАНОВКА РАЗРЕШЕНА ТОЛЬКО КОГДА

A. Все B-*, PR-* и CR-* критерии выполнены и один draft PR содержит воспроизводимое
   evidence; либо
B. Остался настоящий owner action, который физически требует внешнего credential,
   account permission, Telegram token/chat ID, VPS access или необратимого решения
   о реальных средствах.

При B сначала заверши всю независимую работу. Затем создай один точный owner action:
что сделать, почему Codex не может сделать это, как проверить результат и какое
fail-closed поведение действует до разблокировки.

ФИНАЛ ТЕКУЩЕЙ ЦЕЛИ

Один scoped draft PR, в котором:

- docker compose up --build запускает реальный Wave 1 shadow-продукт, а не stub;
- make verify зелёный;
- STATUS.md честно обновлён;
- перечислены пройденные B/PR/CR criteria и команды evidence;
- live по умолчанию и без независимых unlock gates физически невозможен;
- отсутствуют production secrets и withdrawal/transfer code;
- дан один конкретный owner runbook для VPS qualification и минимального canary.
```
