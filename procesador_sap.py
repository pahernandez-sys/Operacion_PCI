import pandas as pd
from datetime import datetime
import re
import io

def extraer_fecha(texto):
    if pd.isna(texto): return None
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', str(texto))
    if match:
        dia, mes, anio = match.groups()
        return f"{anio}{mes}{dia}"
    return None

def guardar_formato_sap_exacto(df, nombre, h2):
    # Formato SAP: Tabuladores (\t) y saltos de línea Windows (\r\n)
    sep = '\t'
    with open(nombre, 'w', encoding='cp1252', newline='\r\n') as f:
        f.write(sep.join(df.columns) + '\n')
        f.write(sep.join(h2) + '\n')
        for _, row in df.iterrows():
            linea = sep.join(map(str, row.values))
            f.write(linea + '\n')

def procesar_sap(archivo_binario):
    contenido = io.BytesIO(archivo_binario)
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

    guardar_formato_sap_exacto(pd.DataFrame(cab), "Salida_Almacen_Cabecera.txt", 
                           ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"])
    guardar_formato_sap_exacto(pd.DataFrame(lin), "Salida_Almacen_Lineas.txt", 
                           ["DocNum", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"])
    
    return doc_num - 1
