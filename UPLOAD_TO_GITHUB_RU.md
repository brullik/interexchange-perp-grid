# Как загрузить fast-track пакет и запустить Codex

## 1. Сначала закройте репозиторий

Репозиторий `brullik/interexchange-perp-grid` при последней проверке был пустым и публичным. Откройте:

```text
GitHub → interexchange-perp-grid → Settings → General
→ Danger Zone → Change repository visibility → Private
```

Даже в private-репозиторий нельзя загружать реальные API-ключи и Telegram-токен.

## 2. Распакуйте архив прямо в корень клона

PowerShell:

```powershell
cd "$HOME\Documents"
git clone https://github.com/brullik/interexchange-perp-grid.git

Expand-Archive `
  -Path "$HOME\Downloads\interexchange-perp-grid-fast-track-v2.zip" `
  -DestinationPath "$HOME\Documents\interexchange-perp-grid" `
  -Force

cd "$HOME\Documents\interexchange-perp-grid"
git status --short
git add --all
git commit -m "Add fast-track product contract and bootstrap"
git push -u origin main
```

Архив уже содержит файлы корня. Не создавайте дополнительную вложенную папку.

## 3. Проверьте GitHub Actions

Во вкладке Actions должен запуститься workflow `ci`. Он выполняет:

```text
ruff check
ruff format --check
mypy
pytest
bootstrap doctor
```

## 4. Запустите Codex одной целью

1. Откройте репозиторий в Codex.
2. При наличии команды включите `/fast on`.
3. Отправьте целиком блок из `CODEX_START_PROMPT_RU.md`.
4. Не дробите первую цель на десятки отдельных задач и не просите Codex сначала написать новые документы.
5. В следующих сессиях используйте `CODEX_RESUME_PROMPT_RU.md`.

## 5. Что не нужно делать сейчас

- не создавайте production API-ключи;
- не переводите средства специально для бота;
- не подключайте семь бирж вручную;
- не добавляйте Redis/PostgreSQL/Kafka;
- не запускайте live до C4 и отдельного canary owner action.

## 6. Что потребуется от владельца позже

Только после C4:

- VPS в выбранном после benchmark регионе;
- Telegram bot token и owner chat ID;
- restricted API credentials без withdrawal на двух canary-биржах;
- IP allowlist;
- явное подтверждение минимального live canary.
