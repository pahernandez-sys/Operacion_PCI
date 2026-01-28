import pandas as pd
from datetime import datetime
import re
from google.colab import files
import io
import base64
import zipfile
from IPython.display import HTML, display

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
            if col1 != "nan" and col3 != "nan" and col1.lower() not in ["area", "division"]: area_actual = col1
            elif col0 != "nan" and col3 == "nan":
                temp_area = re.sub(r'(COBRE|FIBRA|FO|CU|SALIDA|ENTRADA|\d{2}/\d{2}/\d{4})\s*', '', col0, flags=re.IGNORECASE).strip()
                if temp_area: area_actual = temp_area
            if col3 == "nan" or col3.lower() in ["número de artículo"]: continue
            tecnico_actual = col0 if col0 != "nan" else tecnico_actual
            
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

        # --- ESTRUCTURA TXT ---
        f_hoy = datetime.now().strftime("%Y%m%d")
        h_cab = ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"]
        h_lin = ["ParentKey", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"]

        cab_txt = '\t'.join(h_cab) + '\r\n' + '\t'.join(h_cab) + '\r\n'
        lin_txt = '\t'.join(h_lin) + '\r\n' + '\t'.join(h_lin) + '\r\n'
        
        doc_num = 1
        for (tec, area), info in mapeo_datos.items():
            f_doc = info["DocDate"] if info["DocDate"] else f_hoy
            cab_txt += f"{doc_num}\t60\t{f_doc}\t{info['U_DIVISION']}\t{info['U_AREA']}\tMANTENIMIENTO\t{info['U_CONTRATISTA']}\tORIGINAL\t{info['Comments']}\r\n"
            for i, ln in enumerate(info["Lines"]):
                lin_txt += f"{doc_num}\t{i}\t{ln['ItemCode']}\t{ln['Quantity']}\tCAMARONE\t{info['U_CONTRATISTA']}\t{info['U_AREA']}\r\n"
            doc_num += 1

        # --- CREAR ZIP EN MEMORIA ---
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            zip_file.writestr("Salida_Almacen_Cabecera.txt", cab_txt.encode('cp1252'))
            zip_file.writestr("Salida_Almacen_Lineas.txt", lin_txt.encode('cp1252'))
        
        # Codificar ZIP a Base64 para el botón
        b64_zip = base64.b64encode(zip_buffer.getvalue()).decode()
        zip_name = f"Carga_SAP_{f_hoy}.zip"

        # --- INTERFAZ FINAL ---
        print(f"✅ Éxito: {doc_num - 1} folios procesados.")
        
        html_code = f'''
            <div style="background-color: #f0fdf4; border: 2px solid #22c55e; padding: 20px; border-radius: 12px; text-align: center; margin-top: 10px;">
                <p style="color: #166534; font-weight: bold; font-family: sans-serif; margin-bottom: 15px;">📦 Paquete SAP listo</p>
                <a href="data:application/zip;base64,{b64_zip}" download="{zip_name}" 
                   onclick="setTimeout(() => {{ window.location.reload(); }}, 1000);"
                   style="background-color: #22c55e; color: white; padding: 12px 30px; text-decoration: none; border-radius: 8px; font-weight: bold; font-family: sans-serif; display: inline-block;">
                    📥 Descargar archivos y finalizar
                </a>
                <p style="font-size: 11px; color: #666; margin-top: 10px;">(El proceso se detendrá automáticamente tras la descarga)</p>
            </div>
        '''
        display(HTML(html_code))

    except Exception as e:
        print(f"❌ Error: {e}")

procesar_sap_final()
