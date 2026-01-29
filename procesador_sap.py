import pandas as pd
from datetime import datetime
import re
from google.colab import files
import io
import time
from google.colab import output
import ipywidgets as widgets
from IPython.display import display

# Variables globales
data_excel = None
nombre_excel = ""

def extraer_fecha(texto):
    if pd.isna(texto): return None
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', str(texto))
    if match:
        dia, mes, anio = match.groups()
        return f"{anio}{mes}{dia}"
    return None

def procesar_y_descargar(b):
    global data_excel, nombre_excel
    output.clear()
    
    if data_excel is None:
        print("❌ Error: No se detectó archivo. Por favor, reinicia la celda.")
        return

    try:
        print(f"🚀 Procesando '{nombre_excel}'...")
        # Lógica de procesamiento (tu código original)
        contenido = io.BytesIO(data_excel)
        excel_file = pd.ExcelFile(contenido)
        
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
            item_code = col3.split('.')[0].strip()
            cantidad = float(row[5]) if pd.notna(row[5]) else 0
            comentarios = str(row[6]).strip() if pd.notna(row[6]) else ""
            fecha_comentario = extraer_fecha(comentarios)

            clave = (tecnico_actual, area_actual)
            if clave not in mapeo_datos:
                mapeo_datos[clave] = {
                    "U_DIVISION": division, "U_AREA": area_actual,
                    "U_CONTRATISTA": tecnico_actual, "Comments": comentarios,
                    "DocDate": fecha_comentario, "Lines": []
                }
            mapeo_datos[clave]["Lines"].append({"ItemCode": item_code, "Quantity": int(cantidad)})

        cabecera_final, lineas_final = [], []
        doc_num = 1
        fecha_hoy = datetime.now().strftime("%Y%m%d")

        for (tec, area), info in mapeo_datos.items():
            f_doc = info["DocDate"] if info["DocDate"] else fecha_hoy
            cabecera_final.append({
                "DocNum": doc_num, "DocObjectCode": "60", "DocDate": f_doc,
                "U_DIVISION": info["U_DIVISION"], "U_AREA": info["U_AREA"],
                "U_TipoP": "MANTENIMIENTO", "U_CONTRATISTA": info["U_CONTRATISTA"],
                "U_COPIA": "ORIGINAL", "Comments": info["Comments"]
            })
            for idx, ln in enumerate(info["Lines"]):
                lineas_final.append({
                    "ParentKey": doc_num, "LineNum": idx, "ItemCode": ln["ItemCode"],
                    "Quantity": ln["Quantity"], "WarehouseCode": "CAMARONE",
                    "U_CONTRATISTA": info["U_CONTRATISTA"], "U_AREA": info["U_AREA"]
                })
            doc_num += 1

        # Lógica de guardado (Doble Header)
        def guardar(df, nombre, h2):
            sep = ';'
            with open(nombre, 'w', encoding='cp1252', newline='') as f:
                f.write(sep.join(df.columns) + '\n')
                f.write(sep.join(h2) + '\n')
                f.write(df.to_csv(sep=sep, index=False, header=False))

        guardar(pd.DataFrame(cabecera_final), "Salida_Almacen_Cabecera.txt", 
                ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"])
        guardar(pd.DataFrame(lineas_final), "Salida_Almacen_Lineas.txt", 
                ["DocNum", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"])

        print(f"✅ ¡Archivos listos! Se procesaron {doc_num - 1} folios.")
        time.sleep(1)
        files.download("Salida_Almacen_Cabecera.txt")
        time.sleep(1)
        files.download("Salida_Almacen_Lineas.txt")
        print("📥 Descargando...")

    except Exception as e:
        print(f"❌ Error: {e}")

# --- FLUJO PRINCIPAL ---
print("1️⃣ Sube el archivo Excel:")
subida = files.upload()

if subida:
    nombre_excel = list(subida.keys())[0]
    data_excel = subida[nombre_excel]
    
    # Creamos un botón que el usuario debe presionar para continuar
    # Esto 'limpia' el estado de bloqueo de la subida
    print("\n✅ Carga al 100%.")
    btn = widgets.Button(description="2️⃣ Finalizar y Descargar", button_style='success', layout=widgets.Layout(width='300px', height='50px'))
    btn.on_click(procesar_y_descargar)
    display(btn)
