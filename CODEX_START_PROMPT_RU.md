# Стартовая цель для Codex: Aggressive Fast Live V2

```text
/goal

Репозиторий: brullik/interexchange-perp-grid

ЕДИНАЯ ЦЕЛЬ

Максимально быстро и автономно довести текущий проект до laptop-tested live торговли агрессивным межбиржевым симбиозом. Полностью убрать длительную квалификацию из пути к live и не заменять её другим многочасовым ожиданием.

Под «убрать квалификацию» понимается:

- live guard, canary, pilot, risk-stage и supervisor не требуют qualification epoch/file/hash/age;
- отсутствуют требования 12h/24h, 10,000 observations и funding checkpoints;
- старые qualification artifacts не могут ни разрешить, ни запретить live;
- laptop path не запускает qualification task и не имеет action `qualify`;
- вместо этого непосредственно перед live выполняется быстрый exact-hash-bound FAST_LIVE_PREFLIGHT с TTL 600 секунд и single-use intent.

Не отключай обязательные проверки текущего состояния: stale/sequence/depth, fee/funding/metadata, account/margin/position mode, FLAT/open orders/unknown journal, economics, risk, protected paired execution, local unlock, Telegram challenge и explicit owner consent остаются fail-closed.

Сначала прочитай только:

1. AGENTS.md
2. GOAL.md
3. config/AGGRESSIVE_FAST_LIVE_V2.yaml
4. FAST_TRACK_PLAN.md
5. ACCEPTANCE.md
6. верхний Current state и последние релевантные записи STATUS.md

После одного чтения немедленно начинай работу. Не создавай новый PRD, план, matrix, ADR, аудит или статусный документ.

BASELINE И GITHUB

- Получи фактический origin/main и запиши exact baseline SHA.
- Не предполагай, что baseline по пакету всё ещё является HEAD.
- Создай или продолжи одну ветку codex/aggressive-fast-live-v2.
- Создай или продолжи один draft PR.
- Не создавай серию PR по checkpoint.
- После каждого coherent checkpoint запускай focused tests; перед commit запускай make verify.
- После exact-head green required checks получи independent review, устрани P0/P1/P2 и material threads, mark Ready и squash-merge автоматически при unchanged head.

ОБЯЗАТЕЛЬНАЯ РЕАЛИЗАЦИЯ

A1. Удалить active qualification dependencies из config/runtime policy, live guard, canary, pilot, risk-stage, orchestrator, CLI, laptop scripts, scheduled tasks, status/runbook и tests. Сохранять legacy DB/code можно только как unreachable compatibility, если физическое удаление создаёт лишний риск. Tests должны доказать, что legacy artifact не имеет authority.

A2. Реализовать FAST_LIVE_PREFLIGHT: exact merged SHA/config/profile/native runtime/route/account/data-generation/risk-stage; current private capabilities; exchange-verified FLAT; no open/unknown orders; clocks/fresh BBO+L2/sequence/depth; fee/funding/metadata; 1m model; executable economics; route/portfolio/margin/leverage. PASS expires in 600s, single-use, never submits orders.

A3. Реализовать reference spread:

Open  = 10000*ln(Open_A/Open_B)
High  = 10000*ln(High_A/Low_B)
Low   = 10000*ln(Low_A/High_B)
Close = 10000*ln(Close_A/Close_B)

Только synchronized closed UTC 1m, no forward-fill. 5m/15m/1h/4h/1d строятся только из 1m spread bars. Сначала on-demand один Wave 1 route и 30 complete days; не скачивать весь universe до vertical slice.

A4. Historical model: mode/normal zone, positive/negative extremes, q99/q99.9, episodes, convergence, adverse excursion, 24h/7d/30d regime. First laptop live: >=30 complete days, >=10 episodes, >=70% 24h convergence, no regime block.

A5. Grid: levels 20/40/60/80/100%, weights 10/15/20/25/30%, 15% reference stop plus adaptive tail, one level once, one tranche per cycle, fresh-book catch-up, frozen model, reverse-grid exits, 0.25-step re-arm, no sixth tranche, stop executable in replay/shadow/live.

A6. Economics/risk: 1.35x stressed cost, normal minimum 0.15 USDT, canary 0.01 only, positive funding credit 50%, adverse 100% and stress 2x, positive convergence without positive funding. Normal sizing <=4.50 modelled and <=5 hard per route; portfolio <=45/50; actual-fill recalculation.

A7. Один decision core для replay/shadow/canary/pilot. Сохранить protected IOC caps, actual-fill reconciliation, durable journal, restart, residual recovery, third venue, emergency flatten, stable-FLAT и текущие C4/security proofs.

A8. Создать один scripts/laptop-fast-live.ps1 с actions:

verify
onboard
preflight
canary
pilot
status
stop

Нет action qualify. Использовать existing DPAPI/S4U/native runtime/Telegram/supervisor.

Canary: current single-use PASS preflight, local unlock, Telegram challenge, separate owner phrase, one route, one tranche, minimum notional, hard risk <=1 USDT. Success — actual exchange evidence and stable-FLAT. Никакого многочасового post-FLAT ожидания.

Pilot: новый PASS preflight и отдельное owner confirmation, one route, up to five tranches, hard risk <=5 USDT, normal economics. Replay обязан доказать все пять levels; live pilot обязан сделать хотя бы один реальный paired round-trip и закончиться stable-FLAT, но не должен форсировать невыгодные уровни.

После successful pilot немедленно создать ignored state/laptop-fast-live-acceptance.json с accepted=true и exact hashes. Не ждать 8 часов.

МИНИМАЛЬНЫЙ SCOPE

- Не переписывай working execution/recovery/security foundation без failing regression test.
- Не вводи web UI, microservices, Redis, Kafka, Celery, Kubernetes или вторую orchestration system.
- Не расширяй сначала все семь venues. Сделай один полный Wave 1 route, затем только необходимое обобщение.
- Не проводи необязательный refactoring, rename, formatting sweep или cleanup.
- Не создавай новые документы кроме явно требуемых существующими contracts/evidence.
- Не жди рыночную возможность во время coding: используй deterministic replay, а реальный live оставь owner-run path.

LAPTOP И OWNER ACTION

Все software/public/replay/shadow действия выполни автономно и с production_submit_calls=0.

После merge выдай ровно один owner action:

1. локально ввести restricted trade-only/no-withdrawal credentials через existing DPAPI/S4U onboarding;
2. запустить preflight;
3. отдельно подтвердить minimum-notional canary;
4. после stable-FLAT отдельно подтвердить pilot.

Не проси ключи, токены, unlock secret или их значения в чате.

VPS ЗАПРЕЩЁН

Не подключайся к VPS, не загружай файлы, не deploy и не переносись secrets. Разрешено только подготовить fail-closed future handoff, который отказывает без exact state/laptop-fast-live-acceptance.json и exact merged release.

ОСТАНОВКА

Не останавливайся из-за объёма, CI failure, review, обычного технического решения, недоступности одной venue или отсутствия рыночной возможности.

Остановка допустима только когда:

A. A0–A7 завершены, один PR merged, Windows fast-live path готов, и осталось только локальное credential/live-money owner action; либо
B. owner позже выполнил canary/pilot и accepted laptop artifact создан; либо
C. существует реальный внешний blocker, который невозможно устранить code/test/replay/public API/CI/GitHub permissions.

ФИНАЛЬНЫЙ ОТЧЁТ

Сообщи только проверяемое:

- baseline, PR, exact merged/current SHA;
- завершённые plan/acceptance IDs;
- команды и результаты tests/required checks/review;
- production_submit_calls;
- подтверждение отсутствия qualification dependency и long wait;
- fast preflight/laptop wrapper status;
- единственный owner action;
- подтверждение, что VPS не изменялся.
```
