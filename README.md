# tcpref_wsp_pj
# ¿Qué hace este script?

Este script automatiza el envío de tipos de cambio preferenciales (TC) por WhatsApp usando Google Sheets + InfoBip. Leo una hoja con clientes, calculo o recupero el TC que les corresponde, formateo los montos y envío un template de WhatsApp personalizado a cada uno.

# Flujo general del script 🔧 

# 1. Leo dos hojas de Google Sheets:

TC PREF: contiene la lista de clientes.

TC_RULES: contiene las reglas de TC por monto.

# 2. Valido y normalizo los datos:

Limpio el número de teléfono y lo paso a formato internacional (51…).

Verifico que tenga RUC.

Normalizo montos y TC aunque vengan con puntos, comas o símbolos.

# 3. Obtengo el tipo de cambio:

Si la fila ya trae TC, lo uso.

Si no, calculo el TC a partir de la tabla de reglas según el monto y si es compra o venta.

# 4. Calculo los valores “Envía” y “Recibe”:

Para venta: el cliente recibe USD y envía soles.

Para compra: el cliente envía USD y recibe soles.

Formateo los montos (S/ 1,234.56 – $ 999.99).

# 5. Armo los placeholders para el template de WhatsApp:

Nombre

TC

Envía

Recibe

# 6. Envio el mensaje por InfoBip
Construyo el payload con el template, los placeholders, un botón de quick reply y el webhook de notificaciones.

# 7. Registro del envío
Leo el messageId, bulkId y muestro logs de éxito o error por cada cliente.


# 📝 En resumen este script:

Conecta Google Sheets con InfoBip.

Limpia y valida teléfonos, montos y TC.

Calcula el tipo de cambio correcto usando reglas.

Formatea los valores finales para el cliente.

Envía un mensaje de WhatsApp totalmente automatizado usando un template.

Loguea todo para control interno.
