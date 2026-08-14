# Interexchange Perpetual Grid — Fast Track

An asynchronous, single-VPS system for paired convergence trading across equal linear USDT perpetual futures.

## Delivery order

1. Working shadow vertical slice on Binance USD-M, Bybit, and OKX.
2. Canary-ready private execution on Bybit and OKX, with Binance USD-M alternate.
3. Bitget and KuCoin Futures.
4. MEXC and BingX.

The repository intentionally starts lean. Product behavior is defined by:

- `GOAL.md`
- `FAST_TRACK_PLAN.md`
- `ACCEPTANCE.md`
- `STATUS.md`

## Bootstrap verification

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
make verify
interexchange-grid doctor
```

## Docker

```bash
cp .env.example .env
docker compose up --build
```

At bootstrap this runs the safety/health process. Codex replaces the bootstrap command with the continuous Wave 1 shadow application during C0–C3.

## Safety status

- Live trading defaults to disabled.
- No withdrawal or transfer functionality is permitted.
- Never commit `.env`, exchange credentials, Telegram tokens, runtime databases, or market data.
- Real trading is not activated by this bootstrap package.

## Start Codex

Upload this package to the repository and send Codex the contents of `CODEX_START_PROMPT_RU.md`.
