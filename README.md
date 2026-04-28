# OpenClaw Bridge Go

Bridge em Go para conectar no WebSocket do OpenClaw e expor uma API HTTP para envio de mensagens e forwarding de eventos para webhook.

Fluxo:

`Aplicacao Externa -> openclaw-bridge-go (/v1/*) -> OpenClaw Gateway WS -> eventos -> Webhook HTTP`

## Estrutura

```text
cmd/server
internal/app
internal/config
internal/events
internal/httpapi
internal/logger
internal/store
internal/webhook
internal/wsclient
```

## Configuracao

1. Copie `.env.example` para `.env`.
2. Preencha no minimo:
- `OPENCLAW_GATEWAY_TOKEN`
- `OPENCLAW_WS_URL`
- `WEBHOOK_URL`

Defaults recomendados para acesso externo via API:

- `OPENCLAW_CLIENT_ID=openclaw-tui`
- `OPENCLAW_CLIENT_MODE=ui`
- `OPENCLAW_ROLE=operator`
- `OPENCLAW_SCOPES=operator.read,operator.write`

## Execucao local

```bash
go run ./cmd/server
```

## Docker Compose

```bash
docker compose up -d --build
docker compose logs -f openclaw-bridge
```

## API publica da bridge

Endpoints:

- `GET /healthz`
- `GET /readyz`
- `POST /v1/sessions/create`
- `POST /v1/sessions/send`

Observacoes:

- O endpoint de entrada da sua aplicacao e `POST /v1/sessions/send`.
- `WEBHOOK_URL` e o destino de saida dos eventos recebidos do OpenClaw.

### Criar sessao

```bash
curl -X POST https://opc-api.pullse.ia.br/v1/sessions/create \
  -H "Content-Type: application/json" \
  -d '{
    "sessionKey":"design:cliente-a",
    "agentId":"design",
    "customerId":"cliente-a",
    "workspace":"/data/.openclaw/workspaces/design/cliente-a"
  }'
```

### Enviar mensagem (modo classico)

```bash
curl -X POST https://opc-api.pullse.ia.br/v1/sessions/send \
  -H "Content-Type: application/json" \
  -d '{
    "sessionKey":"design:cliente-a",
    "message":"Crie uma arte de casa verde",
    "createSession":true
  }'
```

### Enviar mensagem (isolamento automatico por cliente)

Se `sessionKey` nao for informado, a bridge monta `sessionKey` como `agentId:customerId`.

```bash
curl -X POST https://opc-api.pullse.ia.br/v1/sessions/send \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: req-123" \
  -d '{
    "agentId":"design",
    "customerId":"cliente-a",
    "workspace":"/data/.openclaw/workspaces/design/cliente-a",
    "message":"Crie uma arte de barco vermelho",
    "createSession":true,
    "callbackUrl":"https://seu-callback.exemplo.com/openclaw",
    "stream":false,
    "dedupeKey":"req-123"
  }'
```

## Recursos importantes

- `createSession=true`: cria sessao automaticamente se nao existir.
- `callbackUrl`: sobrescreve `WEBHOOK_URL` para a sessao/chamada.
- `stream=false`: envia apenas evento final (quando detectado) e limpa override da sessao.
- `dedupeKey` ou `X-Idempotency-Key`: evita processar entrada duplicada.
- Deduplicacao de saida: evita callback repetido para o mesmo evento.

## Persistencia (PostgreSQL)

Com `DATABASE_URL` configurado, a bridge persiste:

- configuracao de entrega por sessao (`callbackUrl` e `stream`)
- chaves de dedupe (entrada e saida)

Isso reduz perda/duplicidade em restart de container.

## Handshake OpenClaw

Na conexao WS, a bridge:

1. aguarda `connect.challenge`
2. envia `connect` com token/auth
3. inicia escuta continua de eventos
4. opcionalmente envia `sessions.send` se `AUTO_SEND_ON_CONNECT=true`

## Eventos encaminhados

- `session.message`
- `agent`
- `session.tool`
- `sessions.changed`
- `tick`
- `health`

Cada tipo pode ser ligado/desligado via flags `FORWARD_*` no `.env`.

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
