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
import requests  # ← NUEVA IMPORTACIÓN
import threading  # ← NUEVA IMPORTACIÓN

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

# Función keep-alive para mantener Render despierto
def keep_alive():
    try:
        requests.get("https://firebase-to-sheets.onrender.com", timeout=10)
        print("✅ Keep-alive ping enviado")
    except Exception as e:
        print(f"⚠️  Keep-alive falló: {str(e)} (normal en free tier)")

# Sincronizar datos
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
        
        # ✅ OBTENER todos los datos existentes para encontrar la última fila
        all_data = sheet.get_all_values()
        
        # ✅ ENCONTRAR la última fila con datos
        last_row = len(all_data) + 1  # Empezar después del último dato
        
        # Si solo hay headers, empezar en fila 2
        if len(all_data) <= 1:
            last_row = 2
        
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
        
        # ✅ ESCRIBIR NUEVOS datos DEBAJO de los existentes
        if rows:
            # Encontrar la última fila vacía
            while last_row <= sheet.row_count and any(sheet.row_values(last_row)):
                last_row += 1
            
            # Escribir los nuevos datos
            for i, row in enumerate(rows):
                sheet.update(f'A{last_row + i}:E{last_row + i}', [row])
            
            print(f"✅ {len(rows)} productos agregados debajo (fila {last_row})")
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

# Programar ejecuciones cada 5 minutos (sincronización)
schedule.every(5).minutes.do(sync_data)

# Programar keep-alive cada 10 minutos (mantener despierto)  ← NUEVO
schedule.every(10).minutes.do(keep_alive)

# Primera ejecución
print("⏰ Primera sincronización...")
sync_data()

# Primer keep-alive
print("🔔 Primer keep-alive...")
keep_alive()

print("✅ Aplicación en ejecución. Sincronizando cada 5 minutos + Keep-alive cada 10 minutos...")

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
