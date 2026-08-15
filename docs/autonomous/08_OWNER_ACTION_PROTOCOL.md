# Единственный протокол внешних действий владельца

## Немедленное действие безопасности

Репозиторий должен быть Private до добавления operational evidence. Codex сначала
пытается выполнить изменение сам. Если GitHub scope не позволяет, создаёт action:

```json
{
  "type": "REPOSITORY_MAKE_PRIVATE",
  "blocking": ["SECRETS", "PRODUCTION_EVIDENCE"],
  "unblock_condition": "repository.visibility == private"
}
```

## ONE_TIME_ONBOARDING

Codex имеет право запросить его только после `SOFTWARE_COMPLETE`.

Владелец не выбирает технические параметры. Он только предоставляет фактические
секреты/доступы через локальный wizard:

```bash
sudo ipegctl owner-onboard
```

Wizard собирает:

- VPS domain/IP и SSH/bootstrap доступ, если deployment ещё не выполнен;
- Telegram bot token и owner chat ID;
- restricted Binance/Bybit/OKX keys первой волны;
- позднее venue keys только после соответствующего software gate;
- подтверждение выделенных субаккаунтов;
- подтверждение внесённых средств;
- one-time live-canary consent.

## Требования к exchange keys

- read + futures trade;
- withdrawal disabled;
- transfer disabled;
- wallet/address-book/API-management disabled;
- IP allowlist на VPS;
- отдельный key на каждый dedicated subaccount;
- секреты только `/etc/ipeg/ipeg.env`, owner root, mode 0600;
- никогда Git/GitHub/Codex chat/artifact/log.

## Деньги

Для reference capital 500 USDT средства распределяются вручную. MVP не переводит
средства между биржами. Бот показывает rebalance instruction и приостанавливает
необеспеченную venue.

## Live consent

Не включён в этот master-пакет. Первый canary требует короткоживущий Telegram
challenge и точную phrase. Это единственное финансовое подтверждение владельца.
После canary бот может работать автономно только в текущем stage profile.
