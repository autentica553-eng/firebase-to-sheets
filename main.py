import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from google.oauth2.service_account import Credentials
import schedule
import time
from datetime import datetime
import os
import json
from flask import Flask

# Configurar archivos desde variables de entorno
def setup_environment():
    print("🔧 Configurando entorno...")
    
    # Crear archivo de Firebase desde variable de entorno
    if 'FIREBASE_KEY' in os.environ:
        try:
            firebase_config = json.loads(os.environ['FIREBASE_KEY'])
            with open('firebase-key.json', 'w') as f:
                json.dump(firebase_config, f)
            print("✅ Archivo Firebase creado")
        except Exception as e:
            print(f"❌ Error con Firebase key: {str(e)}")
    
    # Crear archivo de Google Sheets desde variable de entorno
    if 'GOOGLE_SHEETS_KEY' in os.environ:
        try:
            sheets_config = json.loads(os.environ['GOOGLE_SHEETS_KEY'])
            with open('google-sheets-key.json', 'w') as f:
                json.dump(sheets_config, f)
            print("✅ Archivo Google Sheets creado")
        except Exception as e:
            print(f"❌ Error con Google Sheets key: {str(e)}")

# Configurar Firebase
def setup_firebase():
    try:
        if os.path.exists('firebase-key.json'):
            cred = credentials.Certificate('firebase-key.json')
            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            print("✅ Firebase configurado")
            return firestore.client()
        else:
            print("❌ No se encontró firebase-key.json")
            return None
    except Exception as e:
        print(f"❌ Error Firebase: {str(e)}")
        return None

# Configurar Google Sheets
def setup_sheets():
    try:
        if os.path.exists('google-sheets-key.json'):
            # Alcances necesarios
            SCOPES = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Crear credenciales
            creds = Credentials.from_service_account_file(
                'google-sheets-key.json', 
                scopes=SCOPES
            )
            
            client = gspread.authorize(creds)
            print("✅ Google Sheets configurado")
            return client
        else:
            print("❌ No se encontró google-sheets-key.json")
            return None
    except Exception as e:
        print(f"❌ Error Google Sheets: {str(e)}")
        return None

# Sincronizar datos (VERSIÓN QUE PRESERVA FÓRMULAS)
def sync_data():
    print(f"\n🔄 Sincronización: {datetime.now().strftime('%H:%M:%S')}")
    
    db = setup_firebase()
    sheets_client = setup_sheets()
    
    if not db or not sheets_client:
        print("❌ No se pueden sincronizar - Conexiones fallidas")
        return
    
    try:
        # COLECCIÓN: productos
        collection_ref = db.collection('productos')
        docs = collection_ref.stream()
        
        # NOMBRE de tu Google Sheet
        sheet = sheets_client.open("CCB Registros Proceso").sheet1
        
        # ✅ OBTENER todos los datos existentes (para preservar fórmulas)
        all_data = sheet.get_all_values()
        
        # ✅ ENCONTRAR dónde terminan los datos y empiezan las fórmulas
        data_end_row = 1  # Empezar después de headers
        for i, row in enumerate(all_data[1:], start=2):  # Skip header
            if not any(row[1:5]):  # Si las celdas de datos (B-E) están vacías
                data_end_row = i - 1
                break
        else:
            data_end_row = len(all_data)
        
        # ✅ LIMPIAR SOLO las celdas de datos (columnas A-E) - NO toda la hoja
        if data_end_row > 1:  # Si hay datos existentes
            # Solo limpia celdas A2:EX (donde X es la última fila con datos)
            sheet.batch_clear([f"A2:E{data_end_row}"])
            print(f"✅ Celdas limpiadas: A2:E{data_end_row}")
        
        # Recopilar NUEVOS datos de Firebase
        rows = []
        for doc in docs:
            data = doc.to_dict()
            row = [
                doc.id,
                str(data.get('nombre', '')),
                str(data.get('precio', '')),
                str(data.get('stock', '')),
                str(data.get('categoria', ''))
            ]
            rows.append(row)
        
        # Escribir NUEVOS datos (después de la fila 1)
        if rows:
            # ✅ Escribir SOLO en columnas A-E
            cell_list = sheet.range(f"A2:E{len(rows) + 1}")
            
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    cell_list[i * 5 + j].value = value
            
            sheet.update_cells(cell_list)
            print(f"✅ {len(rows)} productos sincronizados")
            print(f"📊 Datos sincronizados: {rows}")
        else:
            print("ℹ️ No hay productos para sincronizar")
            
    except Exception as e:
        print(f"❌ Error REAL en sincronización: {str(e)}")
        import traceback
        traceback.print_exc()

# Configuración inicial
print("🚀 Iniciando aplicación de sincronización...")
setup_environment()

# Programar ejecuciones cada 5 minutos
schedule.every(5).minutes.do(sync_data)

# Primera ejecución
print("⏰ Primera sincronización...")
sync_data()

print("✅ Aplicación en ejecución. Sincronizando productos cada 5 minutos...")

# Crear app de Flask
app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Sincronización Firebase-Sheets activa. Funcionando cada 5 minutos."

# Mantener puerto abierto para Render
if __name__ == '__main__':
    # Ejecutar el scheduler en un hilo separado
    import threading
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    # Iniciar el scheduler en segundo plano
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Iniciar Flask (esto abre el puerto para Render)
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
