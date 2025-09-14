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
import requests
import threading

# Configurar archivos desde variables de entorno
def setup_environment():
    print("🔧 Configurando entorno...")
    
    if 'FIREBASE_KEY' in os.environ:
        try:
            firebase_config = json.loads(os.environ['FIREBASE_KEY'])
            with open('firebase-key.json', 'w') as f:
                json.dump(firebase_config, f)
            print("✅ Archivo Firebase creado")
        except Exception as e:
            print(f"❌ Error con Firebase key: {str(e)}")
    
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
            SCOPES = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
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
        requests.get("https://tu-app.onrender.com", timeout=10)
        print("✅ Keep-alive ping enviado")
    except Exception as e:
        print(f"⚠️ Keep-alive falló: {str(e)}")

# Sincronizar una colección específica
def sync_collection(collection_name, worksheet, existing_ids):
    db = setup_firebase()
    if not db:
        return 0
    
    try:
        collection_ref = db.collection(collection_name)
        docs = collection_ref.stream()
        
        new_rows = []
        
        for doc in docs:
            data = doc.to_dict()
            fecha = data.get('date', '')
            
            # ✅ VERIFICAR POR ID DE FIREBASE (NO POR FECHA)
            if doc.id in existing_ids:
                continue
            
            if collection_name == 'cocimiento':
                # ... (el resto del código igual)
                row = [''] * 13
                row[1] = fecha
                row[3] = data.get('N° Cocimiento (Ej: 102)', '')
                row[4] = data.get('A Tq N° (Ej: 4)', '')
                row[5] = data.get('pH (Mosto Macerado) (Ej: 5.4)', '')
                row[7] = data.get('Extracto original [%] p/p (Primer Mosto) (Ej: 18.5)', '')
                row[9] = data.get('Extracto original [%] p/p (Mosto Frío) (Ej: 16.5)', '')
                row[10] = data.get('pH(Mosto Frío) (Ej: 5.43)', '')
                row[11] = data.get('Color [EBC] (Mosto Frío) (Ej: 8.5)', '')
                row[12] = data.get('Observaciones (Ej: Sin muestra frío)', '')
                
                # ✅ AGREGAR EL ID COMO PRIMERA COLUMNA OCULTA
                row[0] = doc.id  # Columna A - ID oculto
                
                new_rows.append(row)
                
            else:
                # Para las otras colecciones, mantener formato original
                row = [fecha]
                
                if collection_name == 'fermentacion':
                    row.extend([
                        data.get('Tipo (Ej: Autentica)', ''),
                        data.get('N° Cocimiento (Ej: 341-342-343)', ''),
                        data.get('Tq N°(Ej: 7)', ''),
                        data.get('pH (Ej: 4.36)', ''),
                        data.get('Color [EBC] (Ej: 9.5)', ''),
                        data.get('Turbidez [EBC] (Ej: 18.92)', ''),
                        data.get('Extrácto aparente [%] p/p (Ej: 2.70)', ''),
                        data.get('Extrácto original [%] p/p (Ej: 16.0)', '')
                    ])
                elif collection_name == 'tanque_presion':
                    row.extend([
                        data.get('Tipo (Ej: Trimalta )', ''),
                        data.get('N° Cocimiento (Ej:125-126)', ''),
                        data.get('Tp N° (Ej: 2)', ''),
                        data.get('Tq N° (Ej: 9-7-6)', ''),
                        data.get('Sedimentos (0/S/SS/SSS) (EJ: S)', ''),
                        data.get('Color [EBC] (Ej: 7.5)', ''),
                        data.get('Extrácto aparente [%] p/p (Ej: 2.06)', ''),
                        data.get('Volumen total [L] (Ej: 6650)', ''),
                        data.get('Volumen H2O [L] (Ej: 1850)', ''),
                        data.get('Tanque A (Ej: 1)', ''),
                        data.get('Volumen total del Tanque A [L] (Ej: 2650)', ''),
                        data.get('Tanque B (Ej: 14)', ''),
                        data.get('Volumen total del Tanque B [L] (Ej: 1950)', ''),
                        data.get('Tanque C (Ej: 9)', ''),
                        data.get('Volumen total del Tanque C [L] (Ej: 200)', ''),
                        data.get('Observaciones', '')
                    ])
                elif collection_name == 'envasado':
                    row.extend([
                        data.get('Tipo (Ej: Occidental)', ''),
                        data.get('Calibre [ml] (Ej: 620)', ''),
                        data.get('Tq N° (Ej: 10-12)', ''),
                        data.get('Tp N° (Ej: 1)', ''),
                        data.get('N° Cocimiento (Ej: 91-92-95-96)', ''),
                        data.get('Turbidez [EBC] (Ej: 0.3)', ''),
                        data.get('Degustación (OK ; no OK)', ''),
                        data.get('Sedimentos (0/S/SS/SSS) (Ej: 0)', ''),
                        data.get('T set [°C] (Pasteurizadora) (Ej: 69)', ''),
                        data.get('T max [°C] (Pasteurizadora) (Ej: 69.1)', ''),
                        data.get('UP', ''),
                        data.get('NaOH [%] (Lavadora) (Ej: 0.48)', ''),
                        data.get('Observaciones (Ej: Adición 1/2 bolsa soda)', '')
                    ])
                elif collection_name == 'producto_terminado':
                    row.extend([
                        data.get('Código (Envasado/Vencimiento) (Ej: L = 150-00438 / V = 30-5-26)', ''),
                        data.get('Tipo (Ej: Trimalta Quinua)', ''),
                        data.get('Calibre [ml] (Ej: 300)', ''),
                        data.get('Tq N° (Ej: 1-10)', ''),
                        data.get('Tp N° (Ej: 2)', ''),
                        data.get('N° Cocimiento (Ej: 63-64)', ''),
                        data.get('pH (Ej: 4.67)', ''),
                        data.get('Color [EBC] (Ej: 150)', ''),
                        data.get('Extrácto aparente [%] p/p (Ej: 11.2)', ''),
                        data.get('Espuma [seg] (123)', ''),
                        data.get('Sedimentos 0°C (0/S/SS/SSS) (Ej: S)', ''),
                        data.get('Sedimentos 20°C (0/S/SS/SSS) (Ej: SS)', ''),
                        data.get('Observaciones', '')
                    ])
                
                new_rows.append(row)
        
        # Escribir nuevos datos
        if new_rows:
            # Para COCIMIENTO: empezar desde fila 6
            if collection_name == 'cocimiento':
                # Buscar la última fila con datos (empezando desde fila 6)
                existing_data = worksheet.get_all_values()
                last_row = 6  # Empezar en fila 6
                
                # Encontrar la última fila no vacía desde la 6 hacia abajo
                for i in range(5, len(existing_data)):
                    if existing_data[i]:  # Si la fila tiene datos
                        last_row = i + 1
                
                # Escribir datos desde la última fila vacía
                for i, row in enumerate(new_rows):
                    range_start = f'A{last_row + i}'
                    range_end = f'M{last_row + i}'  # Hasta columna M
                    worksheet.update(f'{range_start}:{range_end}', [row])
                    
            else:
                # Para otras colecciones: formato normal
                existing_data = worksheet.get_all_values()
                last_row = len(existing_data) + 1 if len(existing_data) > 1 else 2
                
                for i, row in enumerate(new_rows):
                    range_start = f'A{last_row + i}'
                    range_end = chr(65 + len(row) - 1) + str(last_row + i)
                    worksheet.update(f'{range_start}:{range_end}', [row])
            
            return len(new_rows)
        return 0
            
    except Exception as e:
        print(f"❌ Error en {collection_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0

# Sincronizar todos los datos
def sync_data():
    print(f"\n🔄 Sincronización: {datetime.now().strftime('%H:%M:%S')}")
    
    sheets_client = setup_sheets()
    if not sheets_client:
        print("❌ No se puede sincronizar - Conexión fallida")
        return
    
    try:
        # Abrir la hoja de cálculo
        spreadsheet = sheets_client.open("CCB Registros Proceso")
        
        # Nombres de las hojas
        collections = [
            'cocimiento',
            'fermentacion', 
            'tanque_presion',
            'envasado',
            'producto_terminado'
        ]
        
        total_new = 0
        
        for collection_name in collections:
            try:
                # Obtener la hoja
                try:
                    worksheet = spreadsheet.worksheet(collection_name.capitalize())
                except:
                    print(f"⚠️ Hoja {collection_name} no encontrada, saltando...")
                    continue
                
                # Obtener fechas existentes (primera columna)
                existing_data = worksheet.get_all_values()
                existing_ids = set()
                
                if len(existing_data) > 1:
                    for row in existing_data[1:]:
                        if row and row[0]:
                            existing_dates.add(row[0])
                
                # Sincronizar esta colección
                new_count = sync_collection(collection_name, worksheet, existing_ids)
                total_new += new_count
                
                if new_count > 0:
                    print(f"✅ {new_count} nuevos registros en {collection_name}")
                    
            except Exception as e:
                print(f"❌ Error con hoja {collection_name}: {str(e)}")
                continue
        
        print(f"📊 Total de nuevos registros: {total_new}")
            
    except Exception as e:
        print(f"❌ Error general: {str(e)}")

# Configuración inicial
print("🚀 Iniciando aplicación de sincronización...")
setup_environment()

# Programar ejecuciones
schedule.every(5).minutes.do(sync_data)
schedule.every(10).minutes.do(keep_alive)

# Primera ejecución
print("⏰ Primera sincronización...")
sync_data()
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
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
