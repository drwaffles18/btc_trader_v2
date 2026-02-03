Context Pack: cómo funciona el bot (Railway + flujo)
Servicios en Railway

A) btc_trader_history (one-shot / recovery)

Variables:

GOOGLE_CREDS_BASE64

GOOGLE_SERVICE_ACCOUNT_JSON

GOOGLE_SHEET_ID

Repo root:

/BTC_Trader

Start command:

python scripts/initialize_history_total.py

Propósito:

Cargar (~900) velas históricas a Google Sheets si se pierde el historial.

Estado actual:

Desconectado del repo (ejecución manual / rara).

B) btc_trader_incremental_job (cron cada 5 min)

Variables (Sheets):

GOOGLE_CREDS_BASE64

GOOGLE_SERVICE_ACCOUNT_JSON

GOOGLE_SHEET_ID

Start command:

python scripts/update_incremental.py

Cron:

cada 5 min

Propósito:

Agregar filas a Google Sheets (velas / features / snapshots) incrementalmente.

C) telegram_bot (cron cada 5 min)

Variables:

Binance:

BINANCE_API_KEY_TRADING

BINANCE_API_SECRET_TRADING

USE_MARGIN

DRY_RUN

Google Sheets:

GOOGLE_SERVICE_ACCOUNT_JSON

GOOGLE_SHEET_ID

Telegram:

TELEGRAM_TOKEN

TELEGRAM_CHAT_ID

Paths/estado:

STATE_PATH

TRADE_LOG_PATH

Repo:

drwaffles18/btc_trader_v2

Start command:

python alertas/alert_bot.py

Cron:

cada 5 min

Propósito:

Generar señales, notificar Telegram, y ejecutar trades.

D) btc_trader_v2 (Streamlit UI)

Variables:

GOOGLE_CREDS_BASE64

GOOGLE_SERVICE_ACCOUNT_JSON

GOOGLE_SHEET_ID

PORT

RUN_HISTORICAL_INIT

Start command:

streamlit run app.py --server.port $PORT --server.address 0.0.0.0

Propósito:

Dashboard / visualización del performance y señales.

Flujo operativo (high-level)

Sheets es el “backbone” de data/log.

update_incremental.py mantiene el dataset al día (cada 5 min).

alert_bot.py lee datos (o de Binance o de Sheets, según tu implementación), calcula señal, manda Telegram y, si aplica, ejecuta trade y lo loggea.

app.py lee Sheets para mostrar métricas, trades, señales, etc.
