import pandas as pd
from datetime import datetime
import re
from google.colab import files
import io
import os

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
        for nombre_hoja in excel_file.sheet_names[:2]:
            df_temp = pd.read_excel(excel_file, sheet_name=nombre_hoja, header=None)
            if not df_temp.empty:
                for col_idx in range(7):
                    if col_idx not in df_temp.columns: df_temp[col_idx] = None
                dfs_a_concatenar.append(df_temp.iloc[:, 0:7])

        raw_data = pd.concat(dfs_a_concatenar, ignore_index=True)
        mapeo_datos = {}
        tecnico_actual, area_actual = None, "GENERAL"

        for _, row in raw_data.iterrows():
            col0 = str(row[0]).strip() if pd.notna(row[0]) else ""
            col1 = str(row[1]).strip() if pd.notna(row[1]) else ""
            col3 = str(row[3]).strip() if pd.notna(row[3]) else ""

            if any(x in col0 for x in ["Contratista", "ENTRADAS", "SALIDAS"]): continue
            if not col0 and not col1 and not col3: continue

            if col1 and col3 and col1.lower() not in ["area", "division"]:
                area_actual = col1
            elif col0 and not col3:
                temp_area = re.sub(r'(COBRE|FIBRA|FO|CU|SALIDA|ENTRADA|\d{2}/\d{2}/\d{4})\s*', '', col0, flags=re.IGNORECASE).strip()
                if temp_area: area_actual = temp_area

            if not col3 or col3.lower() in ["nan", "número de artículo"]: continue
            tecnico_actual = col0 if col0 else tecnico_actual
            if not tecnico_actual: continue

            division = str(row[2]).strip() if pd.notna(row[2]) else "METRO"
            item_code = str(col3).split('.')[0].strip()
            
            try:
                cantidad = float(row[5]) if pd.notna(row[5]) else 0
            except:
                cantidad = 0

            clave = (tecnico_actual, area_actual)
            if clave not in mapeo_datos:
                mapeo_datos[clave] = {
                    "U_DIVISION": division, "U_AREA": area_actual,
                    "U_CONTRATISTA": tecnico_actual, "Comments": str(row[6]).strip() if pd.notna(row[6]) else "",
                    "DocDate": extraer_fecha(row[6]), "Lines": []
                }
            mapeo_datos[clave]["Lines"].append({"ItemCode": item_code, "Quantity": cantidad})

        # --- ESCRITURA ---
        doc_num = 1
        f_hoy = datetime.now().strftime("%Y%m%d")
        h_cab = ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"]
        h_lin = ["ParentKey", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"]

        f1, f2 = "Salida_Almacen_Cabecera.txt", "Salida_Almacen_Lineas.txt"
        
        # Guardar físicamente en el entorno antes de intentar descargar
        with open(f1, 'w', encoding='cp1252') as fc, open(f2, 'w', encoding='cp1252') as fl:
            fc.write('\t'.join(h_cab) + '\n' + '\t'.join(h_cab) + '\n')
            fl.write('\t'.join(h_lin) + '\n' + '\t'.join(h_lin) + '\n')
            for (tec, area), info in mapeo_datos.items():
                f_doc = info["DocDate"] if info["DocDate"] else f_hoy
                fc.write(f"{doc_num}\t60\t{f_doc}\t{info['U_DIVISION']}\t{info['U_AREA']}\tMANTENIMIENTO\t{info['U_CONTRATISTA']}\tORIGINAL\t{info['Comments']}\n")
                for i, ln in enumerate(info["Lines"]):
                    fl.write(f"{doc_num}\t{i}\t{ln['ItemCode']}\t{ln['Quantity']}\tCAMARONE\t{info['U_CONTRATISTA']}\t{info['U_AREA']}\n")
                doc_num += 1

        print(f"✅ Archivos generados correctamente. {doc_num-1} documentos.")
        
        # Método de descarga forzada para evitar el congelamiento
        for f in [f1, f2]:
            files.download(f)
            print(f"⬇️ Descargando: {f}")

    except Exception as e:
        print(f"❌ Error durante el proceso: {e}")

procesar_sap_final()
