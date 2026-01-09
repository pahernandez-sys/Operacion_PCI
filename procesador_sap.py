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

def procesar_sap_colab_final():
    print("📂 Selecciona el archivo Excel:")
    uploaded = files.upload()
    if not uploaded: return

    archivo_entrada = list(uploaded.keys())[0]

    try:
        contenido_archivo = io.BytesIO(uploaded[archivo_entrada])
        excel_file = pd.ExcelFile(contenido_archivo)
        nombres_hojas = excel_file.sheet_names
        
        dfs_a_concatenar = []
        for i in range(min(2, len(nombres_hojas))):
            df_temp = pd.read_excel(excel_file, sheet_name=i, header=None)
            if df_temp.empty: continue
            
            # Asegurar 7 columnas
            for col_idx in range(7):
                if col_idx not in df_temp.columns: df_temp[col_idx] = None
            
            dfs_a_concatenar.append(df_temp.iloc[:, 0:7])

        if not dfs_a_concatenar:
            print("❌ No hay datos válidos.")
            return

        raw_data = pd.concat(dfs_a_concatenar, ignore_index=True)
        mapeo_datos = {}
        tecnico_actual = None
        area_actual = "GENERAL"

        for _, row in raw_data.iterrows():
            col0 = str(row[0]).strip() if pd.notna(row[0]) else ""
            
            # --- NUEVA LÓGICA DE DETECCIÓN DE ÁREA ---
            # Si la fila parece un encabezado de sección (ej. contiene "COBRE", "FO", "XALAPA" y poco más)
            if col0 and pd.isna(row[3]):
                # Limpiamos palabras comunes para dejar solo el nombre del área
                area_limpia = re.sub(r'(COBRE|FIBRA|FO|CU)\s*', '', col0, flags=re.IGNORECASE).strip()
                if area_limpia:
                    area_actual = area_limpia
                continue

            # Saltar si no hay código de ítem (columna D)
            if pd.isna(row[3]) or str(row[3]).lower() == "nan": continue

            # Identificar Técnico
            tecnico_actual = col0 if col0 else tecnico_actual
            division = str(row[2]).strip() if pd.notna(row[2]) else "METRO"
            item_code = str(row[3]).split('.')[0].strip()

            try:
                cantidad = float(row[5]) if pd.notna(row[5]) else 0
            except:
                cantidad = 0

            comentarios = str(row[6]).strip() if pd.notna(row[6]) else ""
            fecha_comentario = extraer_fecha(comentarios)

            clave = (tecnico_actual, area_actual)

            if clave not in mapeo_datos:
                mapeo_datos[clave] = {
                    "U_DIVISION": division,
                    "U_AREA": area_actual,
                    "U_CONTRATISTA": tecnico_actual,
                    "Comments": comentarios,
                    "DocDate": fecha_comentario,
                    "Lines": []
                }

            mapeo_datos[clave]["Lines"].append({"ItemCode": item_code, "Quantity": cantidad})

        # Generación de TXT (Cabecera y Líneas)
        cabecera_final, lineas_final = [], []
        doc_num = 1
        fecha_hoy = datetime.now().strftime("%Y%m%d")

        for (tec, area), info in mapeo_datos.items():
            f_doc = info["DocDate"] if info["DocDate"] else fecha_hoy
            
            cabecera_final.append({
                "DocNum": doc_num, "ObjType": "60", "DocDate": f_doc,
                "U_DIVISION": info["U_DIVISION"], "U_AREA": info["U_AREA"],
                "U_TipoP": "MANTENIMIENTO", "U_CONTRATISTA": info["U_CONTRATISTA"],
                "U_COPIA": "ORIGINAL", "Comments": info["Comments"]
            })

            for idx, ln in enumerate(info["Lines"]):
                lineas_final.append({
                    "ParentKey": doc_num, "LineNum": idx, "ItemCode": ln["ItemCode"],
                    "Quantity": ln["Quantity"], "WhsCode": "CAMARONE",
                    "U_CONTRATISTA": info["U_CONTRATISTA"], "U_AREA": info["U_AREA"]
                })
            doc_num += 1

        if not cabecera_final:
            print("⚠️ No se generaron registros.")
            return

        pd.DataFrame(cabecera_final).to_csv("Salida_Almacen_Cabecera.txt", index=False, sep='\t')
        pd.DataFrame(lineas_final).to_csv("Salida_Almacen_Lineas.txt", index=False, sep='\t')

        print(f"✅ Éxito: {doc_num - 1} documentos creados.")
        files.download("Salida_Almacen_Cabecera.txt")
        files.download("Salida_Almacen_Lineas.txt")

    except Exception as e:
        print(f"❌ Error: {e}")
