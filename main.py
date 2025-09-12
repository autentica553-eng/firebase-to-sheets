import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from google.oauth2.service_account import Credentials
import schedule
import time
from datetime import datetime
import os
import json

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

# Sincronizar datos
def sync_data():
    print(f"\n🔄 Sincronización: {datetime.now().strftime('%H:%M:%S')}")
    
    db = setup_firebase()
    sheets_client = setup_sheets()
    
    if not db or not sheets_client:
        print("❌ No se pueden sincronizar - Conexiones fallidas")
        return
    
    try:
        # COLECCIÓN: productos (¡MODIFICADO!)
        collection_ref = db.collection('productos')
        docs = collection_ref.stream()
        
        # NOMBRE de tu Google Sheet (cambia si es necesario)
        sheet = sheets_client.open("Mi Base de Datos Firebase").sheet1
        
        # ENCABEZADOS para productos (¡MODIFICADO!)
        headers = ['ID', 'Nombre', 'Precio', 'Stock', 'Categoría']
        sheet.clear()
        sheet.append_row(headers)
        
        # Recopilar datos
        rows = []
        for doc in docs:
            data = doc.to_dict()
            row = [
                doc.id,
                data.get('nombre', ''),
                data.get('precio', ''),
                data.get('stock', ''),
                data.get('categoria', '')
            ]
            rows.append(row)
        
        # Escribir datos
        if rows:
            sheet.append_rows(rows)
            print(f"✅ {len(rows)} productos sincronizados")
        else:
            print("ℹ️ No hay productos para sincronizar")
            
    except Exception as e:
        print(f"❌ Error en sincronización: {str(e)}")

# Configuración inicial
print("🚀 Iniciando aplicación de sincronización...")
setup_environment()

# Programar ejecuciones cada 5 minutos
schedule.every(5).minutes.do(sync_data)

# Primera ejecución
print("⏰ Primera sincronización...")
sync_data()

print("✅ Aplicación en ejecución. Sincronizando productos cada 5 minutos...")

# Mantener el script ejecutándose
while True:
    schedule.run_pending()
    time.sleep(60)

# =============================================================================
# AGREGAR ESTO AL FINAL DEL ARCHIVO - PARA RENDER WEB SERVICE
# =============================================================================
from flask import Flask
app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Sincronización Firebase-Sheets activa. Funcionando cada 5 minutos."

# Mantener puerto abierto para Render
if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
    
