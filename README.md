# B-Bot Secure (Baseline)

Repositorio **plantilla** para crear múltiples instancias de tu bot sin tocar el código base.

## Instalación

```bash
npm install
node index.js
```

Escanea el QR en WhatsApp (se guardará en `qr_code/`).

## Variables de entorno

Copia `.env.example` a `.env` y completa tus valores. **No subas `.env`** al repo.

## Repos derivados

Marca este repo como *Template* en GitHub y usa **Use this template** para crear nuevos bots.
Cada instancia tendrá su propio `.env` y su propia sesión local (`qr_code/`).

## Estructura (importante)
- `data/` y `qr_code/` están **ignorados** por Git. No subas sesiones ni datos sensibles.
- Puedes personalizar el mensaje de `.bot` con `BOT_INFO_ENABLED` y `BOT_INFO_TEXT`.
