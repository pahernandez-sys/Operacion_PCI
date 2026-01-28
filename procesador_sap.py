import pandas as pd
from datetime import datetime
import re
from google.colab import files
import io
from IPython.display import display, HTML

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

        # --- GENERACIÓN DE DATAFRAMES ---
        cab_rows, lin_rows = [], []
        doc_num = 1
        f_hoy = datetime.now().strftime("%Y%m%d")

        for (tec, area), info in mapeo_datos.items():
            f_doc = info["DocDate"] if info["DocDate"] else f_hoy
            cab_rows.append({
                "DocNum": doc_num, "ObjType": "60", "DocDate": f_doc, "U_DIVISION": info["U_DIVISION"],
                "U_AREA": info["U_AREA"], "U_TipoP": "MANTENIMIENTO", "U_CONTRATISTA": info["U_CONTRATISTA"],
                "U_COPIA": "ORIGINAL", "Comments": info["Comments"]
            })
            for idx, ln in enumerate(info["Lines"]):
                lin_rows.append({
                    "ParentKey": doc_num, "LineNum": idx, "ItemCode": ln["ItemCode"], "Quantity": ln["Quantity"],
                    "WhsCode": "CAMARONE", "U_CONTRATISTA": info["U_CONTRATISTA"], "U_AREA": info["U_AREA"]
                })
            doc_num += 1

        # Doble encabezado estilo Pandas
        df_cab = pd.DataFrame(cab_rows)
        df_lin = pd.DataFrame(lin_rows)
        df_cab_final = pd.concat([pd.DataFrame([df_cab.columns.tolist()], columns=df_cab.columns), df_cab], ignore_index=True)
        df_lin_final = pd.concat([pd.DataFrame([df_lin.columns.tolist()], columns=df_lin.columns), df_lin], ignore_index=True)

        # Guardar archivos en el disco de Colab
        df_cab_final.to_csv("Salida_Almacen_Cabecera.txt", index=False, sep='\t', lineterminator='\r\n', encoding='cp1252')
        df_lin_final.to_csv("Salida_Almacen_Lineas.txt", index=False, sep='\t', lineterminator='\r\n', encoding='cp1252')

        print(f"✅ Éxito: {doc_num - 1} folios listos.")
        
        # --- SOLUCIÓN: BOTÓN DE DESCARGA ---
        html_code = """
        <div style="background-color: #e6fffa; border: 1px solid #38b2ac; padding: 15px; border-radius: 8px; margin-top: 10px;">
            <p style="color: #2c7a7b; font-weight: bold; margin-bottom: 10px;">📦 Los archivos están listos para SAP:</p>
            <button onclick="downloadFiles()" style="background-color: #38b2ac; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-size: 16px;">
                Descargar TXTs ahora
            </button>
        </div>
        <script>
            function downloadFiles() {
                const files = ['Salida_Almacen_Cabecera.txt', 'Salida_Almacen_Lineas.txt'];
                files.forEach((file, index) => {
                    setTimeout(() => {
                        const link = document.createElement('a');
                        link.href = '/content/' + file;
                        link.download = file;
                        document.body.appendChild(link);
                        link.click();
                        document.body.removeChild(link);
                    }, index * 1000);
                });
            }
        </script>
        """
        display(HTML(html_code))

    except Exception as e:
        print(f"❌ Error: {e}")

procesar_sap_final()
