import pandas as pd
from datetime import datetime
import re
import io
import time
from google.colab import files as colab_files

def extraer_fecha(texto):
    if pd.isna(texto): return None
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', str(texto))
    if match:
        dia, mes, anio = match.groups()
        return f"{anio}{mes}{dia}"
    return None

def guardar_formato_sap_final(df, nombre, h2):
    # Usamos tabulador (\t) y saltos de línea Windows (\r\n) para SAP
    sep = '\t'
    with open(nombre, 'w', encoding='cp1252', newline='\r\n') as f:
        # Escribir Fila 1 (Encabezados técnicos)
        f.write(sep.join(df.columns) + '\n')
        # Escribir Fila 2 (Etiquetas de Template)
        f.write(sep.join(h2) + '\n')
        
        # Escribir Datos fila por fila para evitar errores de lineterminator en pandas
        for _, row in df.iterrows():
            linea = sep.join(map(str, row.values))
            f.write(linea + '\n')

try:
    nombre_archivo = list(subida.keys())[0]
    print(f"⚙️ Procesando con formato SAP (Tabuladores): {nombre_archivo}...")
    
    contenido = io.BytesIO(subida[nombre_archivo])
    excel_file = pd.ExcelFile(contenido)
    dfs = []
    for i in range(min(2, len(excel_file.sheet_names))):
        df_t = pd.read_excel(excel_file, sheet_name=i, header=None)
        if df_t.empty: continue
        for c in range(7): 
            if c not in df_t.columns: df_t[c] = None
        dfs.append(df_t.iloc[:, 0:7])

    raw = pd.concat(dfs, ignore_index=True)
    mapeo = {}
    tec, area = None, "GENERAL"

    for _, row in raw.iterrows():
        c0, c1, c3 = [str(row[i]).strip() if pd.notna(row[i]) else "" for i in [0, 1, 3]]
        if any(x in c0 for x in ["Contratista", "ENTRADAS", "SALIDAS"]) or (not c0 and not c1 and not c3): continue
        if c1 and c3 and c1.lower() not in ["area", "division"]: area = c1
        elif c0 and not c3:
            t_a = re.sub(r'(COBRE|FIBRA|FO|CU|SALIDA|ENTRADA|\d{2}/\d{2}/\d{4})\s*', '', c0, flags=re.IGNORECASE).strip()
            if t_a: area = t_a
        if not c3 or c3.lower() in ["nan", "número de artículo"]: continue
        tec = c0 if c0 else tec
        if not tec: continue

        item = c3.split('.')[0].strip()
        cant = int(float(row[5])) if pd.notna(row[5]) else 0
        coment = str(row[6]).strip() if pd.notna(row[6]) else ""
        f_coment = extraer_fecha(coment)
        
        clave = (tec, area)
        if clave not in mapeo:
            mapeo[clave] = {"div": str(row[2]).strip() if pd.notna(row[2]) else "METRO", "area": area, "tec": tec, "com": coment, "date": f_coment, "lines": []}
        mapeo[clave]["lines"].append({"item": item, "qty": cant})

    cab, lin = [], []
    doc_num = 1
    hoy = datetime.now().strftime("%Y%m%d")

    for (t, a), info in mapeo.items():
        f_d = info["date"] if info["date"] else hoy
        cab.append({"DocNum": doc_num, "DocObjectCode": "60", "DocDate": f_d, "U_DIVISION": info["div"], "U_AREA": info["area"], "U_TipoP": "MANTENIMIENTO", "U_CONTRATISTA": info["tec"], "U_COPIA": "ORIGINAL", "Comments": info["com"]})
        for idx, l in enumerate(info["lines"]):
            lin.append({"ParentKey": doc_num, "LineNum": idx, "ItemCode": l["item"], "Quantity": l["qty"], "WarehouseCode": "CAMARONE", "U_CONTRATISTA": info["tec"], "U_AREA": info["area"]})
        doc_num += 1

    # Guardado manual (Formato 48 garantizado)
    guardar_formato_sap_final(pd.DataFrame(cab), "Salida_Almacen_Cabecera.txt", 
                           ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"])
    guardar_formato_sap_final(pd.DataFrame(lin), "Salida_Almacen_Lineas.txt", 
                           ["DocNum", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"])

    print(f"✅ ¡Proceso terminado! {doc_num - 1} documentos generados.")
    colab_files.download("Salida_Almacen_Cabecera.txt")
    colab_files.download("Salida_Almacen_Lineas.txt")

except Exception as e:
    print(f"❌ Error crítico: {e}")
