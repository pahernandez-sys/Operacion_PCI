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
            if any(x in col0 for x in ["Contratista", "ENTRADAS", "SALIDAS"]) or (not col0 and not col1 and not col3): continue
            if col1 and col3 and col1.lower() not in ["area", "division"]: area_actual = col1
            elif col0 and not col3:
                temp_area = re.sub(r'(COBRE|FIBRA|FO|CU|SALIDA|ENTRADA|\d{2}/\d{2}/\d{4})\s*', '', col0, flags=re.IGNORECASE).strip()
                if temp_area: area_actual = temp_area
            if not col3 or col3.lower() in ["nan", "número de artículo"]: continue
            tecnico_actual = col0 if col0 else tecnico_actual
            if not tecnico_actual: continue
            
            division = str(row[2]).strip() if pd.notna(row[2]) else "METRO"
            item_code = str(col3).split('.')[0].strip()
            cantidad = 0
            try: cantidad = float(row[5]) if pd.notna(row[5]) else 0
            except: pass

            clave = (tecnico_actual, area_actual)
            if clave not in mapeo_datos:
                mapeo_datos[clave] = {
                    "U_DIVISION": division, "U_AREA": area_actual,
                    "U_CONTRATISTA": tecnico_actual, "Comments": str(row[6]).strip() if pd.notna(row[6]) else "",
                    "DocDate": extraer_fecha(row[6]), "Lines": []
                }
            mapeo_datos[clave]["Lines"].append({"ItemCode": item_code, "Quantity": cantidad})

        # --- ESTRUCTURA SAP ---
        f_hoy = datetime.now().strftime("%Y%m%d")
        h_cab = ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"]
        h_lin = ["ParentKey", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"]

        # 1. Generar contenido Cabecera
        cab_content = '\t'.join(h_cab) + '\n' + '\t'.join(h_cab) + '\n'
        # 2. Generar contenido Líneas
        lin_content = '\t'.join(h_lin) + '\n' + '\t'.join(h_lin) + '\n'
        
        doc_num = 1
        for (tec, area), info in mapeo_datos.items():
            f_doc = info["DocDate"] if info["DocDate"] else f_hoy
            cab_content += f"{doc_num}\t60\t{f_doc}\t{info['U_DIVISION']}\t{info['U_AREA']}\tMANTENIMIENTO\t{info['U_CONTRATISTA']}\tORIGINAL\t{info['Comments']}\n"
            for i, ln in enumerate(info["Lines"]):
                lin_content += f"{doc_num}\t{i}\t{ln['ItemCode']}\t{ln['Quantity']}\tCAMARONE\t{info['U_CONTRATISTA']}\t{info['U_AREA']}\n"
            doc_num += 1

        # ESCRIBIR ARCHIVOS AL DISCO DE COLAB
        with open("Salida_Almacen_Cabecera.txt", "w", encoding="cp1252") as f: f.write(cab_content)
        with open("Salida_Almacen_Lineas.txt", "w", encoding="cp1252") as f: f.write(lin_content)

        print(f"✅ {doc_num-1} folios procesados.")
        
        # DESCARGA
        from google.colab import files as colab_files
        colab_files.download("Salida_Almacen_Cabecera.txt")
        colab_files.download("Salida_Almacen_Lineas.txt")
        print("🚀 Descarga enviada al navegador.")

    except Exception as e:
        print(f"❌ Error: {e}")

procesar_sap_final()
