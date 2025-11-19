import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import re
import time

# Configuración para acceder al template de un sheet para enviar el tc pref a los clientes.
SHEET_ID = "SHEET ID"
SHEET_NAME = "TC PREF"
INFOBIP_API_KEY = "CREDENCIAL INFOBIP ACÁ"
INFOBIP_URL = "end point de infobip url"
REMITENTE_WHATSAPP = "51908828010"
TEMPLATE_NAME = "alarma_tc_python"

# Identificador de campaña (bulkId)
bulk_id = f"TC_PREF_PJ"

# Autenticación Google Sheets
creds = Credentials.from_service_account_file(
    'CREDENCIALES ACÁ',
    scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
data = sheet.get_all_records()
rules_sheet = client.open_by_key(SHEET_ID).worksheet("TC_RULES")
rules_data = rules_sheet.get_all_records(expected_headers=[
    "Monto (USD)", "Compra", "Venta"
])

def formatear_monto(simbolo, monto):
    return f"{simbolo} {float(monto):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def parse_num(valor):
    if isinstance(valor, (int, float)):
        return float(valor)
    valor = str(valor)
    valor = re.sub(r"[^\d,.\-]", "", valor)
    valor = valor.strip()
    if "," in valor and "." in valor:
        valor = valor.replace(".", "").replace(",", ".")
    else:
        valor = valor.replace(",", ".")
    return float(valor)

def get_tc_pref_from_table(monto, detalle, tabla):
    tabla_ordenada = sorted(tabla, key=lambda x: float(x['Monto (USD)']))
    tc = None
    for fila in tabla_ordenada:
        if monto >= float(fila['Monto (USD)']):
            if detalle == "compra":
                tc = float(str(fila['Compra']).replace("S/", "").strip())
            elif detalle == "venta":
                tc = float(str(fila['Venta']).replace("S/", "").strip())
        else:
            break
    return tc

for i, row in enumerate(data, start=2):  # Empieza en 2 por el header
    telefono = str(row.get("Teléfono", "")).strip().replace(" ", "")
    if not telefono:
        print(f"[{i}] Sin teléfono. Fila omitida.")
        continue
    telefono = ''.join(filter(str.isdigit, telefono))
    if not telefono.startswith("51") and len(telefono) == 9:
        telefono = "51" + telefono
    if len(telefono) != 11:
        print(f"[{i}] Teléfono inválido: {telefono}. Fila omitida.")
        continue

    nombre = row.get("Nombre", "")
    detalle = row.get("Detalle", "").lower()
    banco = row.get("Banco", "")
    monto = row.get("Monto (USD)", 0)
    tc = row.get("tc", 0)
    ruc = str(row.get("Ruc", "")).strip()
    if not ruc:
        print(f"[{i}] Sin RUC. Fila omitida.")
        continue

    try:
        monto = parse_num(monto)
        tc = parse_num(tc)
    except Exception:
        tc = 0  # Si no hay tc, lo ponemos en 0 para buscar en reglas

    # Si el TC está vacío o es 0, busca en la tabla de reglas
    if not tc or tc == 0:
        tc_pref = get_tc_pref_from_table(monto, detalle, rules_data)
        if tc_pref:
            tc = tc_pref
        else:
            print(f"[{i}] Sin TC y sin regla. Fila omitida.")
            continue

    if detalle == "venta":
        recibe = monto
        envia = monto * tc
        placeholder_envia = formatear_monto("S/", envia)
        placeholder_recibe = formatear_monto("$", recibe)
    elif detalle == "compra":
        envia = monto
        recibe = monto * tc
        placeholder_envia = formatear_monto("$", envia)
        placeholder_recibe = formatear_monto("S/", recibe)
    else:
        print(f"[{i}] Detalle inválido: {detalle}. Fila omitida.")
        continue

    placeholders = [
        nombre,
        str(tc),
        placeholder_envia,
        placeholder_recibe
    ]

    payload = {
    "bulkId": bulk_id,
    "messages": [{
        "from": REMITENTE_WHATSAPP,
        "to": f"+{telefono}",
        "content": {
            "templateName": TEMPLATE_NAME,
            "templateData": {
                "body": {
                    "placeholders": placeholders
                },
                "buttons": [{
                    "type": "QUICK_REPLY",
                    "parameter": f"RUC_{ruc}, Envía_{placeholder_envia}, Recibe_{placeholder_recibe}, TC_{tc}, {banco}"
                }]
            },
            "language": "es"
        },
        "notifyUrl": "https://hooks.zapier.com/hooks/catch/23820608/uuygxwc/"  # <--- tu webhook aquí
    }]
}

    headers_req = {
        "Authorization": f"App {INFOBIP_API_KEY}",
        "Content-Type": "application/json"
    }

    print(f"[{i}] Enviando a: {telefono} | Nombre: {nombre} | Monto: {monto} | TC: {tc} | Detalle: {detalle}")

    try:
        response = requests.post(INFOBIP_URL, json=payload, headers=headers_req)
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        resp_json = response.json()
        message_id = ""
        group_id = ""
        if "messages" in resp_json and len(resp_json["messages"]) > 0:
            message_id = resp_json["messages"][0].get("messageId", "")
            group_id = resp_json["messages"][0].get("bulkId", "")
        print(f"[{i}] ✅ Envío exitoso | Message ID: {message_id} | Group ID: {group_id} | {timestamp}")
    except Exception as e:
        print(f"[{i}] ❌ Error enviando a {telefono}: {e}")
