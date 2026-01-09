import pandas as pd
from datetime import datetime
import os
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
    print("📂 Por favor, selecciona el archivo Excel que deseas procesar:")
    uploaded = files.upload()

    if not uploaded:
        print("❌ No se subió ningún archivo.")
        return

    archivo_entrada = list(uploaded.keys())[0]

    try:
        print(f"🔄 Procesando {archivo_entrada}...")
        contenido_archivo = io.BytesIO(uploaded[archivo_entrada])

        excel_file = pd.ExcelFile(contenido_archivo)
        nombres_hojas = excel_file.sheet_names
        
        if len(nombres_hojas) < 2:
            print(f"❌ El archivo debe tener al menos 2 pestañas. Encontradas: {len(nombres_hojas)}")
            return

        print(f"📖 Leyendo Hoja 1: '{nombres_hojas[0]}' y Hoja 2: '{nombres_hojas[1]}'")
        
        # --- SOLUCIÓN AL ERROR DE COLUMNAS ---
        # Leemos sin 'usecols' primero para evitar el error de límites
        df_fo_raw = pd.read_excel(excel_file, sheet_name=0, header=None)
        df_cu_raw = pd.read_excel(excel_file, sheet_name=1, header=None)

        # Forzamos que tengan al menos 7 columnas (A a la G) rellenando con vacío si faltan
        def asegurar_columnas(df):
            for i in range(7):
                if i not in df.columns:
                    df[i] = None
            return df.iloc[:, 0:7] # Retornamos exactamente de A a G

        df_fo = asegurar_columnas(df_fo_raw)
        df_cu = asegurar_columnas(df_cu_raw)
        # --------------------------------------

        raw_data = pd.concat([df_fo, df_cu], ignore_index=True).dropna(how='all')

        mapeo_datos = {}
        tecnico_actual = None
        area_actual = "GENERAL"

        for _, row in raw_data.iterrows():
            # Validación de seguridad para filas vacías o incompletas
            if len(row) < 4: continue 

            if pd.isna(row[1]) and pd.isna(row[3]) and pd.notna(row[0]):
                area_actual = str(row[0]).replace("COBRE ", "").strip()
                continue

            if pd.isna(row[3]) or str(row[3]).lower() == "nan": continue

            nombre = str(row[0]).strip() if pd.notna(row[0]) else tecnico_actual
            division = str(row[2]).strip() if pd.notna(row[2]) else "METRO"
            item_code = str(row[3]).split('.')[0].strip()

            try:
                cantidad = float(row[5]) if pd.notna(row[5]) else 0
            except:
                cantidad = 0

            comentarios = str(row[6]).strip() if pd.notna(row[6]) else ""
            fecha_comentario = extraer_fecha(comentarios)

            tecnico_actual = nombre
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

        f_cabecera = "Salida_Almacen_Cabecera.txt"
        f_lineas = "Salida_Almacen_Lineas.txt"

        pd.DataFrame(cabecera_final).to_csv(f_cabecera, index=False, sep='\t')
        pd.DataFrame(lineas_final).to_csv(f_lineas, index=False, sep='\t')

        print(f"✅ Éxito: Se generaron {doc_num - 1} folios.")
        print("📥 Iniciando descarga automática...")

        files.download(f_cabecera)
        files.download(f_lineas)

    except Exception as e:
        import traceback
        print(f"❌ Error durante el proceso: {e}")
        # traceback.print_exc() # Descomenta esto si necesitas ver la línea exacta del error

