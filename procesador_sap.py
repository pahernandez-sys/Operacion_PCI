import pandas as pd
from datetime import datetime
import re
from google.colab import files
import io

def extraer_fecha(texto):
    if pd.isna(texto): return None
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', str(texto))
    if match:
        dia, mes, anio = match.groups()
        return f"{anio}{mes}{dia}"
    return None

def procesar_sap_final():
    print("📂 Seleccionando archivo...")
    uploaded = files.upload()
    if not uploaded: return
    archivo_entrada = list(uploaded.keys())[0]

    try:
        contenido_archivo = io.BytesIO(uploaded[archivo_entrada])
        excel_file = pd.ExcelFile(contenido_archivo)
        
        dfs_a_concatenar = []
        for i in range(min(2, len(excel_file.sheet_names))):
            df_temp = pd.read_excel(excel_file, sheet_name=i, header=None)
            if df_temp.empty: continue
            for col_idx in range(7):
                if col_idx not in df_temp.columns: df_temp[col_idx] = None
            dfs_a_concatenar.append(df_temp.iloc[:, 0:7])

        raw_data = pd.concat(dfs_a_concatenar, ignore_index=True)
        mapeo_datos = {}
        tecnico_actual, area_actual = None, "GENERAL"

        for _, row in raw_data.iterrows():
            col0, col1, col3 = str(row[0]).strip(), str(row[1]).strip(), str(row[3]).strip()
            if any(x in col0 for x in ["Contratista", "ENTRADAS", "SALIDAS"]) or (col0=="nan" and col1=="nan" and col3=="nan"): continue
            
            if col1 != "nan" and col3 != "nan" and col1.lower() not in ["area", "division"]:
                area_actual = col1
            elif col0 != "nan" and col3 == "nan":
                temp_area = re.sub(r'(COBRE|FIBRA|FO|CU|SALIDA|ENTRADA|\d{2}/\d{2}/\d{4})\s*', '', col0, flags=re.IGNORECASE).strip()
                if temp_area: area_actual = temp_area

            if col3 == "nan" or col3.lower() in ["número de artículo"]: continue
            tecnico_actual = col0 if col0 != "nan" else tecnico_actual
            if not tecnico_actual: continue

            division = str(row[2]).strip() if pd.notna(row[2]) else "METRO"
            item_code = col3.split('.')[0].strip()
            try: cantidad = float(row[5]) if pd.notna(row[5]) else 0
            except: cantidad = 0

            clave = (tecnico_actual, area_actual)
            if clave not in mapeo_datos:
                mapeo_datos[clave] = {
                    "U_DIVISION": division, "U_AREA": area_actual, "U_CONTRATISTA": tecnico_actual,
                    "Comments": str(row[6]).strip() if pd.notna(row[6]) else "",
                    "DocDate": extraer_fecha(row[6]), "Lines": []
                }
            mapeo_datos[clave]["Lines"].append({"ItemCode": item_code, "Quantity": cantidad})

        # --- ESTRUCTURA DE DATOS ---
        cab_rows, lin_rows = [], []
        doc_num = 1
        f_hoy = datetime.now().strftime("%Y%m%d")

        for (tec, area), info in mapeo_datos.items():
            f_doc = info["DocDate"] if info["DocDate"] else f_hoy
            cab_rows.append([doc_num, "60", f_doc, info["U_DIVISION"], info["U_AREA"], "MANTENIMIENTO", info["U_CONTRATISTA"], "ORIGINAL", info["Comments"]])
            for idx, ln in enumerate(info["Lines"]):
                lin_rows.append([doc_num, idx, ln["ItemCode"], ln["Quantity"], "CAMARONE", info["U_CONTRATISTA"], info["U_AREA"]])
            doc_num += 1

        # Nombres de columnas
        h_cab = ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"]
        h_lin = ["ParentKey", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"]

        # --- EL TRUCO DEL DOBLE ENCABEZADO ---
        # Creamos los DataFrames incluyendo los encabezados como las dos primeras filas
        df_cab = pd.DataFrame([h_cab, h_cab] + cab_rows)
        df_lin = pd.DataFrame([h_lin, h_lin] + lin_rows)

        # Guardar sin encabezados automáticos de Pandas (porque ya los pusimos nosotros)
        df_cab.to_csv("Salida_Almacen_Cabecera.txt", index=False, header=False, sep='\t', lineterminator='\r\n', encoding='cp1252')
        df_lin.to_csv("Salida_Almacen_Lineas.txt", index=False, header=False, sep='\t', lineterminator='\r\n', encoding='cp1252')

        print(f"✅ Éxito: {doc_num - 1} folios generados.")
        
        # Descarga automática (como al principio)
        files.download("Salida_Almacen_Cabecera.txt")
        files.download("Salida_Almacen_Lineas.txt")

    except Exception as e:
        print(f"❌ Error: {e}")

procesar_sap_final()
