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
        
        dfs_a_concatenar = []

        # Iteramos por las primeras 2 hojas por posición
        for i in range(min(2, len(nombres_hojas))):
            try:
                # Leemos la hoja completa sin restricciones de columnas primero
                df_temp = pd.read_excel(excel_file, sheet_name=i, header=None)
                
                if df_temp.empty:
                    print(f"⚠️ Hoja {i+1} ('{nombres_hojas[i]}') está vacía. Saltando...")
                    continue

                # Aseguramos que tenga 7 columnas (A-G) para no romper la lógica de índices
                for col_idx in range(7):
                    if col_idx not in df_temp.columns:
                        df_temp[col_idx] = None
                
                # Nos quedamos solo con las columnas A-G (0 a 6)
                dfs_a_concatenar.append(df_temp.iloc[:, 0:7])
                print(f"✅ Hoja {i+1} ('{nombres_hojas[i]}') cargada correctamente.")
                
            except Exception as e:
                print(f"⚠️ No se pudo procesar la hoja {i+1}: {e}")

        if not dfs_a_concatenar:
            print("❌ No se encontró información válida en ninguna de las hojas.")
            return

        # Unimos la información encontrada
        raw_data = pd.concat(dfs_a_concatenar, ignore_index=True).dropna(how='all')

        mapeo_datos = {}
        tecnico_actual = None
        area_actual = "GENERAL"

        for _, row in raw_data.iterrows():
            # Saltar filas que no tengan suficientes columnas
            if len(row) < 4: continue 

            # Lógica de detección de Área
            if pd.isna(row[1]) and pd.isna(row[3]) and pd.notna(row[0]):
                area_actual = str(row[0]).replace("COBRE ", "").strip()
                continue

            # Saltar si no hay código de ítem
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

        # 3. Estructura final
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
            print("ℹ️ El proceso terminó pero no se generaron registros (revisa el formato del Excel).")
            return

        f_cabecera = "Salida_Almacen_Cabecera.txt"
        f_lineas = "Salida_Almacen_Lineas.txt"

        pd.DataFrame(cabecera_final).to_csv(f_cabecera, index=False, sep='\t')
        pd.DataFrame(lineas_final).to_csv(f_lineas, index=False, sep='\t')

        print("-" * 30)
        print(f"✅ Éxito: Se generaron {doc_num - 1} folios.")
        print("📥 Iniciando descarga automática...")

        files.download(f_cabecera)
        files.download(f_lineas)

    except Exception as e:
        print(f"❌ Error inesperado: {e}")
