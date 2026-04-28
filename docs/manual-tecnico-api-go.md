# Manual Técnico - OpenClaw API Go Bridge

## 1. Visão geral

A `api-go` é uma bridge entre:

- OpenClaw Gateway via WebSocket (`ws://` ou `wss://`)
- APIs HTTP externas (entrada via endpoints REST)
- Webhook HTTP (saída de eventos em tempo real)

Fluxo principal:

`Aplicação Externa -> API Go (/v1/*) -> OpenClaw WS -> eventos -> callback webhook`

## 2. Arquitetura técnica

Principais módulos:

- `cmd/server`: bootstrap da aplicação
- `internal/config`: carrega e valida `.env`
- `internal/wsclient`: handshake WS, RPC (`sessions.send`, `sessions.create`) e escuta de eventos
- `internal/app`: orquestração entre WS e webhook, regras de forward
- `internal/httpapi`: endpoints HTTP públicos
- `internal/events`: extração/normalização de eventos
- `internal/webhook`: entrega HTTP com retry/backoff
- `internal/store`: persistência em PostgreSQL (callback por sessão e dedupe)

## 3. Dependências

Dependências Go:

- `github.com/gorilla/websocket`
- `github.com/joho/godotenv`

Infra/execução:

- Go estável atual
- Docker / Docker Compose (opcional, recomendado)
- Traefik (opcional, para domínio público + TLS)
- PostgreSQL (recomendado para persistência e dedupe)

## 4. Pré-requisitos OpenClaw

Para funcionamento da API:

1. Gateway OpenClaw ativo e acessível via `OPENCLAW_WS_URL`
2. Token de gateway válido (`OPENCLAW_GATEWAY_TOKEN`)
3. Cliente/mode compatíveis no handshake

Configuração recomendada no bridge:

- `OPENCLAW_CLIENT_ID=openclaw-tui`
- `OPENCLAW_CLIENT_MODE=ui`
- `OPENCLAW_ROLE=operator`
- `OPENCLAW_SCOPES=operator.read,operator.write`

### 4.1 Ajuste de segurança para alguns cenários

Em alguns ambientes, para liberar fluxos sem identidade de device no Control UI, pode ser necessário:

- `gateway.controlUi.dangerouslyDisableDeviceAuth=true` no `openclaw.json`

Observação:

- Essa flag reduz segurança. Recomendado usar somente quando necessário e planejar migração para fluxo com identidade/token de device.

## 5. Configuração `.env`

Exemplo base:

```env
OPENCLAW_WS_URL=wss://seu-openclaw.exemplo.com
OPENCLAW_GATEWAY_TOKEN=seu_token
OPENCLAW_MIN_PROTOCOL=3
OPENCLAW_MAX_PROTOCOL=3

OPENCLAW_CLIENT_ID=openclaw-tui
OPENCLAW_CLIENT_VERSION=1.0.0
OPENCLAW_CLIENT_PLATFORM=linux
OPENCLAW_CLIENT_MODE=ui
OPENCLAW_ROLE=operator
OPENCLAW_SCOPES=operator.read,operator.write
OPENCLAW_LOCALE=pt-BR
OPENCLAW_USER_AGENT=openclaw-bridge-go/1.0.0
OPENCLAW_SESSION_KEY=agent:main:main

WEBHOOK_URL=https://seu-callback.exemplo.com/openclaw
WEBHOOK_AUTH_HEADER=
WEBHOOK_AUTH_VALUE=
WEBHOOK_TIMEOUT_SECONDS=15

FORWARD_SESSION_MESSAGE=true
FORWARD_SESSION_TOOL=true
FORWARD_SESSIONS_CHANGED=true
FORWARD_TICK=false
FORWARD_HEALTH=false

AUTO_SEND_ON_CONNECT=false
AUTO_SEND_MESSAGE=

WS_RECONNECT=true
WS_RECONNECT_DELAY_SECONDS=5
HTTP_RETRY_COUNT=3
HTTP_RETRY_DELAY_MS=1000
LOG_LEVEL=info

TRAEFIK_HOST=opc-api.seu-dominio.com
TRAEFIK_CERTRESOLVER=letsencrypt
BRIDGE_HTTP_PORT=8080

DATABASE_URL=postgres://openclaw:openclaw@postgres:5432/openclaw_bridge?sslmode=disable
DELIVERY_TTL_SECONDS=600
DEDUPE_RECORD_TTL_SECONDS=86400
```

## 6. Endpoints HTTP

Base URL exemplo:

`https://opc-api.seu-dominio.com`

### 6.1 Health

- `GET /healthz`
- `GET /readyz`

Resposta:

```json
{ "status": "ok" }
```

### 6.2 Criar sessão

- `POST /v1/sessions/create`

Payload opcional:

```json
{
  "sessionKey": "agent:main:minha-sessao"
}
```

Se `sessionKey` não for enviado, o OpenClaw pode gerar uma chave automaticamente (dependendo da versão/política).

Resposta esperada:

```json
{
  "ok": true,
  "sessionKey": "agent:main:minha-sessao",
  "sessionId": "uuid-opcional",
  "requestId": "create-123"
}
```

### 6.3 Enviar mensagem

- `POST /v1/sessions/send`

Payload:

```json
{
  "sessionKey": "agent:main:minha-sessao",
  "message": "Olá OpenClaw",
  "createSession": true,
  "callbackUrl": "https://webhook.site/seu-id",
  "stream": false,
  "dedupeKey": "msg-12345"
}
```

Campos:

- `sessionKey`: chave da sessão
- `message`: texto obrigatório
- `createSession`: se `true`, cria sessão automaticamente se não existir
- `callbackUrl`: se informado, sobrescreve `WEBHOOK_URL` para essa sessão/chamada
- `stream`:
  - `true` (default): envia eventos normalmente
  - `false`: tenta encaminhar apenas evento final
- `dedupeKey`: chave de idempotência para evitar processar entrada duplicada

Também é aceito idempotência por header:

- `X-Idempotency-Key: msg-12345`

Resposta:

```json
{
  "ok": true,
  "requestId": "send-123"
}
```

## 7. Regras de callback/webhook

O OpenClaw responde nativamente pelo WS. O callback HTTP é responsabilidade da bridge.

A bridge:

1. recebe eventos WS (`type=event`)
2. aplica filtro (`FORWARD_*`)
3. normaliza payload
4. envia `POST` para webhook

Com persistência ativa (PostgreSQL):

- `callbackUrl` e `stream` por sessão são salvos em banco
- configuração sobrevive a restart do container

Logs de referência:

- `webhook dispatching`
- `webhook delivered`
- `webhook delivery failed`

## 8. Eventos encaminhados

Atualmente a bridge suporta e/ou trata:

- `session.message`
- `agent`
- `session.tool`
- `sessions.changed`
- `tick`
- `health`

Obs.: O evento `agent` em versões mais novas do OpenClaw pode representar a resposta principal do agente.

## 9. Payload enviado ao webhook

Formato:

```json
{
  "source": "openclaw",
  "receivedAt": "2026-04-28T00:00:00Z",
  "eventType": "session.message",
  "sessionKey": "agent:main:minha-sessao",
  "requestId": "send-123",
  "raw": {},
  "normalized": {}
}
```

## 10. Execução

### 10.1 Local (Go)

```bash
go run ./cmd/server
```

### 10.2 Docker Compose

```bash
docker compose up -d --build
docker compose logs -f openclaw-bridge
```

## 11. Deploy e atualização

Quando mudar config (`.env`):

```bash
docker compose up -d --force-recreate
```

Quando nova imagem for publicada:

```bash
docker compose pull openclaw-bridge
docker compose up -d --force-recreate openclaw-bridge
```

Subida completa (com Postgres local):

```bash
docker compose up -d
docker compose ps
```

## 12. Troubleshooting

### 12.1 `failed to deliver message to openclaw`

Verificar logs da bridge e causa raiz:

- sessão inválida
- escopo ausente
- payload incompatível

### 12.2 `failed to create openclaw session`

Possíveis causas:

- política do OpenClaw para criação de sessão
- variação de formato na resposta RPC
- versão antiga da imagem em execução

### 12.3 Callback não recebe nada

Checklist:

1. evento chegou no WS (`ws event received`)
2. passou no filtro (`should_forward=true`)
3. dispatch iniciou (`webhook dispatching`)
4. entrega ok (`webhook delivered`) ou falhou (`webhook delivery failed`)

### 12.4 Mensagens repetidas (entrada/saída)

Entrada:

- use `dedupeKey` no payload ou `X-Idempotency-Key` no header
- a API registra chave de dedupe e ignora repetidas

Saída:

- bridge registra fingerprint de evento e evita callback duplicado
- TTL controlado por `DEDUPE_RECORD_TTL_SECONDS`

## 13. Segurança recomendada

1. Use `wss://` sempre que possível
2. Proteja token de gateway
3. Restrinja acesso do endpoint público por WAF/rate-limit
4. Use `WEBHOOK_AUTH_HEADER/WEBHOOK_AUTH_VALUE` se callback exigir autenticação
5. Evite manter `dangerouslyDisableDeviceAuth` sem necessidade

## 14. Persistência e redundância (atual)

Implementado:

- Persistência de callback por sessão (`session_delivery`)
- Registro de dedupe (`dedupe_keys`)
- Limitação de perda em restart do container

Ainda recomendado para evolução futura:

- outbox de callbacks com worker dedicado
- replay/reprocessamento de eventos pendentes
- métricas de entrega e alertas automáticos

## 15. Referências internas do projeto

- [README.md](/e:/apps/CLIENTES/OPENCLAW/OPENCLAW-API-2026-V1/api-go/README.md)
- [openclaw-adjustments-for-api.md](/e:/apps/CLIENTES/OPENCLAW/OPENCLAW-API-2026-V1/api-go/docs/openclaw-adjustments-for-api.md)
