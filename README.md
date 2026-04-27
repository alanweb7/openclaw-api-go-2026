# OpenClaw Bridge Go

Bridge em Go para conectar no WebSocket do OpenClaw e encaminhar eventos em tempo real para um webhook HTTP.

Fluxo:

`OpenClaw Gateway WS -> openclaw-bridge-go -> Webhook HTTP`

## Estrutura

```text
cmd/server
internal/app
internal/config
internal/events
internal/logger
internal/webhook
internal/wsclient
```

## Configuracao

1. Copie `.env.example` para `.env`.
2. Preencha ao menos:
- `OPENCLAW_GATEWAY_TOKEN`
- `WEBHOOK_URL`

As demais variaveis possuem default seguro conforme o prompt.

## Execucao local

```bash
make run
```

ou:

```bash
go run ./cmd/server
```

## Docker Compose

```bash
docker compose up -d --build
docker compose logs -f openclaw-bridge
```

## Handshake OpenClaw

Na conexao o bridge:

1. aguarda `connect.challenge`
2. envia `connect` com token/auth
3. inicia escuta continua de eventos
4. opcionalmente envia `sessions.send` se `AUTO_SEND_ON_CONNECT=true`

## Exemplo de payload recebido do WS

```json
{
  "type": "event",
  "event": "session.message",
  "params": {
    "sessionKey": "agent:main:guardian",
    "text": "Ola, mundo"
  }
}
```

## Exemplo de payload enviado ao webhook

```json
{
  "source": "openclaw",
  "receivedAt": "2026-04-27T17:00:00Z",
  "eventType": "session.message",
  "sessionKey": "agent:main:guardian",
  "requestId": "send-1",
  "raw": {
    "type": "event",
    "event": "session.message",
    "params": {
      "sessionKey": "agent:main:guardian",
      "text": "Ola, mundo"
    }
  },
  "normalized": {
    "kind": "agent_message",
    "text": "Ola, mundo",
    "agent": "guardian"
  }
}
```

## Filtro de forwarding

Eventos suportados:

- `session.message`
- `session.tool`
- `sessions.changed`
- `tick`
- `health`

Cada tipo pode ser ligado/desligado por flags `FORWARD_*` no `.env`.

## Reconexao

Se o WS cair:

1. loga o erro
2. aguarda `WS_RECONNECT_DELAY_SECONDS`
3. reconecta
4. refaz handshake
5. retoma forwarding

## Comandos uteis

```bash
make fmt
make test
make build
make docker-build
make docker-up
make docker-down
```

## Workflow de imagem no GitHub

O workflow `/.github/workflows/docker-image.yml` faz build/push para `ghcr.io` em push para `main` (e tags), com suporte multi-arquitetura (`linux/amd64` e `linux/arm64`).
