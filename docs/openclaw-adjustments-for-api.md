# OpenClaw Adjustments for External API Access

This guide explains how to unlock OpenClaw for external API usage through `openclaw-bridge`, including the required gateway and bridge settings.

## Objective

Enable this flow:

`External App -> HTTPS (Traefik) -> openclaw-bridge -> OpenClaw Gateway (WS/WSS)`

## 1. OpenClaw Gateway Requirements

In OpenClaw config (`openclaw.json`), ensure:

- `gateway.auth.mode` is configured (token or password)
- `gateway.controlUi.dangerouslyDisableDeviceAuth=true` when using Control UI client identity without device signature

Example:

```json
{
  "gateway": {
    "auth": {
      "mode": "token",
      "token": "YOUR_GATEWAY_TOKEN"
    },
    "controlUi": {
      "dangerouslyDisableDeviceAuth": true
    }
  }
}
```

Important:

- This flag reduces security for Control UI identity checks.
- Recommended only when you need fast external API unlock.
- Safer long-term approach: implement device identity/device token auth in the bridge.

## 2. Bridge .env Required Values

In `/docker/api-openclaw/.env` (or your runtime `.env`), set:

```env
OPENCLAW_WS_URL=wss://openclaw-fjso.srv1586938.hstgr.cloud
OPENCLAW_GATEWAY_TOKEN=YOUR_GATEWAY_TOKEN

OPENCLAW_CLIENT_ID=openclaw-tui
OPENCLAW_CLIENT_MODE=ui
OPENCLAW_ROLE=operator
OPENCLAW_SCOPES=operator.read,operator.write

TRAEFIK_HOST=opc-api.pullse.ia.br
TRAEFIK_CERTRESOLVER=letsencrypt
BRIDGE_HTTP_PORT=8080
```

Notes:

- `OPENCLAW_CLIENT_ID` and `OPENCLAW_CLIENT_MODE` must be compatible with gateway policy.
- `operator.write` is required for `sessions.send`.

## 3. Compose/Traefik Requirements

`docker-compose.yml` labels should expose the bridge on Traefik websecure:

- `traefik.enable=true`
- router rule with your host
- entrypoint `websecure`
- tls certresolver configured
- service port pointing to `BRIDGE_HTTP_PORT` (default 8080)

## 4. Restart Commands

From API folder on VPS:

```bash
cd /docker/api-openclaw
docker compose pull openclaw-bridge
docker compose up -d --force-recreate openclaw-bridge
docker compose logs -f openclaw-bridge
```

If OpenClaw gateway config changed:

```bash
cd /docker/openclaw-fjso
docker compose restart openclaw
```

## 5. External API Test

Health:

```bash
curl -i https://opc-api.pullse.ia.br/healthz
```

Send message:

```bash
curl -i -X POST https://opc-api.pullse.ia.br/v1/sessions/send \
  -H "Content-Type: application/json" \
  -d '{
    "sessionKey":"agent:main:guardian",
    "message":"teste externo"
  }'
```

Expected success:

- HTTP 200
- payload like: `{"ok":true,"requestId":"send-..."}`

## 6. Common Errors and Fixes

### `failed to deliver message to openclaw`

Check bridge logs for root cause:

```bash
docker compose logs --since=10m openclaw-bridge
```

### `missing scope: operator.write`

Fix:

- Ensure `OPENCLAW_SCOPES=operator.read,operator.write`
- Ensure client identity and gateway policy are allowing declared scopes

### `CONTROL_UI_DEVICE_IDENTITY_REQUIRED`

Fix options:

1. Enable `gateway.controlUi.dangerouslyDisableDeviceAuth=true` (quick unlock)
2. Implement device identity auth in the bridge (recommended long-term)

### `invalid sessions.send params: must have required property 'key'`

Cause:

- Bridge is sending old param shape (`sessionKey`) to OpenClaw method.

Fix:

- Bridge must send `params.key` internally when calling `sessions.send`.

## 7. Security Checklist Before Production

- Use strong gateway token.
- Restrict public host access (WAF/IP rules if possible).
- Monitor bridge and gateway logs.
- Plan migration from `dangerouslyDisableDeviceAuth` to device-token/device-signature flow.

## 8. Quick Validation Checklist

- DNS points to VPS
- Traefik certificate active
- `openclaw-bridge` container is Up
- bridge logs show `handshake completed`
- `POST /v1/sessions/send` returns HTTP 200
