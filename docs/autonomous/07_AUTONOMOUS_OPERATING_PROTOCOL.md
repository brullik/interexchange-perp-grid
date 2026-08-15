# Протокол автономной работы Codex

## 1. Не задавать владельцу технические вопросы

Все технические решения уже установлены пакетом. При неопределённости применять:

```text
fail closed → quarantine affected venue/route → continue independent work
```

## 2. Единственный mutable status

Создать в корне:

```text
AUTONOMOUS_STATUS.json
```

Он валидируется schema и содержит:

- exact branch/PR/SHA;
- current phase/state;
- completed criteria;
- active work;
- blockers;
- latest CI/artifact;
- next deterministic action;
- owner action, если действительно нужен.

Не плодить status-документы.

## 3. Owner action

Создавать `OWNER_ACTION.json` только для внешнего действия, которое невозможно
выполнить кодом или GitHub permissions. Не использовать owner action для:

- выбора библиотеки;
- архитектуры;
- исправления тестов;
- документации;
- API contract research;
- simulator/replay;
- CI;
- создания PR/issue;
- рефакторинга.

## 4. Git/GitHub полномочия

Codex уполномочен:

- создавать/изменять issues, labels, branches и PR;
- запускать и анализировать CI;
- исправлять review comments;
- переводить собственный PR в Ready;
- squash-merge собственный PR после всех locked gates;
- создавать tags/releases и GHCR images;
- продолжать следующим scoped PR без ожидания владельца.

Запрещено merge при:

- red/missing required check;
- unresolved P0/P1 review;
- head changed after evidence;
- live enabled by default;
- secret finding;
- missing rollback;
- mismatch artifact/head/image.

## 5. Review separation

Для каждого merge использовать три роли с отдельными контекстами:

1. Implementer.
2. Adversarial reviewer — только spec/diff/tests, не принимает заявления implementer.
3. Release verifier — exact SHA/CI/artifacts/manifests.

Reviewer findings P0/P1 возвращаются implementer автоматически.

## 6. Работа при внешнем blocker

Если нет VPS/credentials:

- завершить все adapters, simulator, deployment scripts и docs;
- построить release artifacts;
- создать один consolidated owner action;
- не прекращать другие независимые phases.

## 7. Экономия времени и токенов

- не переписывать уже зелёные модули без конкретного дефекта;
- сначала vertical slice, затем расширение;
- один scoped PR на логически независимый phase;
- не создавать длинные ADR: exception фиксируется коротким JSON;
- использовать exact test manifests, а не повторные общие аудиты;
- кэшировать official API research в `docs/exchanges/<venue>.md`;
- не повторять одни и те же инструкции в каждом prompt.

## 8. Exception protocol

Отклонение от locked решения разрешено только при доказанной несовместимости
официального API. Создать:

```text
DECISION_EXCEPTION.json
```

с полями official source, observed behavior, affected venue, selected predefined
fallback и tests. Нельзя ослаблять risk/safety или спрашивать владельца о выборе.
