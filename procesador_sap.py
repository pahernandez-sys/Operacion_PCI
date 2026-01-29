import pandas as pd
from datetime import datetime
import re
from google.colab import files
import io
import time
from google.colab import output
import ipywidgets as widgets
from IPython.display import display

def extraer_fecha(texto):
    if pd.isna(texto): return None
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', str(texto))
    if match:
        dia, mes, anio = match.groups()
        return f"{anio}{mes}{dia}"
    return None

def guardar_txt_sap(df, nombre_archivo, h2):
    # Separador punto y coma para que Excel lo abra directo en columnas
    sep = ';' 
    with open(nombre_archivo, 'w', encoding='cp1252', newline='') as f:
        f.write(sep.join(df.columns) + '\n')
        f.write(sep.join(h2) + '\n')
        # Generar contenido CSV como string para evitar errores de lineterminator
        csv_content = df.to_csv(sep=sep, index=False, header=False)
        f.write(csv_content)

def ejecutar_proceso_completo(b):
    output.clear()
    print("📂 Selecciona tu archivo Excel...")
    uploaded = files.upload()
    
    if not uploaded:
        print("⚠️ No se seleccionó archivo.")
        return

    archivo_nombre = list(uploaded.keys())[0]
    
    try:
        print(f"⚙️ Procesando '{archivo_nombre}'...")
        contenido = io.BytesIO(uploaded[archivo_nombre])
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
        tecnico_actual = None
        area_actual = "GENERAL"

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

        # Headers fila 2
        h2_cab = ["DocNum", "ObjType", "DocDate", "U_DIVISION", "U_AREA", "U_TipoP", "U_CONTRATISTA", "U_COPIA", "Comments"]
        h2_lin = ["DocNum", "LineNum", "ItemCode", "Quantity", "WhsCode", "U_CONTRATISTA", "U_AREA"]

        # Guardar
        guardar_txt_sap(pd.DataFrame(cabecera_final), "Salida_Almacen_Cabecera.txt", h2_cab)
        guardar_txt_sap(pd.DataFrame(lineas_final), "Salida_Almacen_Lineas.txt", h2_lin)

        print(f"✅ ¡Éxito! {doc_num - 1} documentos listos.")
        print("📥 Iniciando descargas...")
        
        time.sleep(1)
        files.download("Salida_Almacen_Cabecera.txt")
        time.sleep(1)
        files.download("Salida_Almacen_Lineas.txt")

    except Exception as e:
        print(f"❌ Error: {e}")

# Crear botón para disparar el proceso
boton = widgets.Button(description="Seleccionar Excel y Procesar", button_style='primary', layout=widgets.Layout(width='300px'))
boton.on_click(ejecutar_proceso_completo)
display(boton)
