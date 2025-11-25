# 🚫 ESTE SCRIPT YA NO SE USA
# ----------------------------------------------------------
# Motivo:
# Este script descargaba TODO el histórico desde 2024-12-01,
# generando más de 100k velas por símbolo + 5 hojas,
# lo cual rompe el límite de 10M celdas de Google Sheets.
#
# Ahora se usa exclusivamente:
#   scripts/initialize_history_total.py
#
# Cualquier ejecución accidental debe detenerse de inmediato.
# ----------------------------------------------------------

raise RuntimeError(
    "❌ initialize_history.py está deshabilitado. "
    "Usa initialize_history_total.py para cargar el histórico compacto."
)
