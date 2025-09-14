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
        # IMPORTANTE: Cambia esta URL por la de tu app en Render
        requests.get("https://tu-app.onrender.com", timeout=10)
        print("✅ Keep-alive ping enviado")
    except Exception as e:
        print(f"⚠️ Keep-alive falló: {str(e)}")

# Funciones de cálculo para fermentación
def calcular_peso_esp(extracto_aparente):
    """Calcula Peso Esp = 0.99995121 + 0.00392802 * Extracto aparente"""
    try:
        return 0.99995121 + 0.00392802 * float(extracto_aparente)
    except:
        return ""

def calcular_gaf(extracto_original, extracto_aparente):
    """Calcula GAF = ((Extracto original - Extracto Aparente) / Extracto Original) * 100"""
    try:
        eo = float(extracto_original)
        ea = float(extracto_aparente)
        return ((eo - ea) / eo) * 100
    except:
        return ""

def calcular_alcohol_peso(extracto_original, extracto_aparente):
    """Calcula Alcohol Peso = (100*(Extracto original-Extracto aparente)/(100*2.5233-Extracto Original*1.1266))"""
    try:
        eo = float(extracto_original)
        ea = float(extracto_aparente)
        return (100 * (eo - ea)) / (100 * 2.5233 - eo * 1.1266)
    except:
        return ""

def calcular_alcohol_volumen(alcohol_peso, peso_esp):
    """Calcula Alcohol volumen = Alcohol Peso * Peso Esp / 0.791"""
    try:
        return float(alcohol_peso) * float(peso_esp) / 0.791
    except:
        return ""

def calcular_extracto_real(extracto_original, alcohol_peso):
    """Calcula Extracto Real = ((Extracto original*(1.0665*Alcohol Peso+100))/(100))-2.0665*Alcohol Peso"""
    try:
        eo = float(extracto_original)
        ap = float(alcohol_peso)
        return ((eo * (1.0665 * ap + 100)) / 100) - 2.0665 * ap
    except:
        return ""

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
                # COCIMIENTO - desde fila 6
                row = [''] * 13
                row[0] = doc.id  # Columna A: ID oculto
                row[1] = fecha  # Columna B: Fecha
                row[2] = data.get('Tipo (Ej: Judas) holi', '')
                row[3] = data.get('N° Cocimiento (Ej: 102)', '')  # Columna D
                row[4] = data.get('A Tq N° (Ej: 4)', '')  # Columna E
                row[5] = data.get('pH (Mosto Macerado) (Ej: 5.4)', '')  # Columna F
                row[6] = ''  # Columna G (vacía)
                row[7] = data.get('Extracto original [%] p/p (Primer Mosto) (Ej: 18.5)', '')  # Columna H
                row[8] = ''  # Columna I (vacía)
                row[9] = data.get('Extracto original [%] p/p (Mosto Frío) (Ej: 16.5)', '')  # Columna J
                row[10] = data.get('pH(Mosto Frío) (Ej: 5.43)', '')  # Columna K
                row[11] = data.get('Color [EBC] (Mosto Frío) (Ej: 8.5)', '')  # Columna L
                row[12] = data.get('Observaciones (Ej: Sin muestra frío)', '')  # Columna M
                
                new_rows.append(row)
                
            elif collection_name == 'fermentacion':
                # FERMENTACIÓN - desde fila 6 (columnas B, C, E, F, G, H, I, J)
                extracto_aparente = data.get('Extrácto aparente [%] p/p (Ej: 2.70)', '')
                extracto_original = data.get('Extrácto original [%] p/p (Ej: 16.0)', '')
                
                # Realizar cálculos
                peso_esp = calcular_peso_esp(extracto_aparente) if extracto_aparente else ""
                gaf = calcular_gaf(extracto_original, extracto_aparente) if extracto_original and extracto_aparente else ""
                alcohol_peso = calcular_alcohol_peso(extracto_original, extracto_aparente) if extracto_original and extracto_aparente else ""
                alcohol_volumen = calcular_alcohol_volumen(alcohol_peso, peso_esp) if alcohol_peso and peso_esp else ""
                extracto_real = calcular_extracto_real(extracto_original, alcohol_peso) if extracto_original and alcohol_peso else ""
                
                row = [''] * 15  # A-O (15 columnas para incluir cálculos)
                row[0] = doc.id  # Columna A: ID oculto
                row[1] = fecha  # Columna B: Fecha
                row[2] = data.get('Tipo (Ej: Autentica)', '')  # Columna C: Tipo
                row[3] = data.get('N° Cocimiento (Ej: 341-342-343)', '')  # Columna D
                row[4] = data.get('Tq N°(Ej: 7)', '')  # Columna E: Tq N°
                row[5] = data.get('pH (Ej: 4.36)', '')  # Columna F: pH
                row[6] = data.get('Color [EBC] (Ej: 9.5)', '')  # Columna G: Color
                row[7] = data.get('Turbidez [EBC] (Ej: 18.92)', '')  # Columna H: Turbidez
                row[8] = extracto_aparente  # Columna I: Ext. Aparente
                row[9] = extracto_original  # Columna J: Ext. Original
                row[10] = extracto_real  # Columna K: Extracto Real
                row[11] = alcohol_peso  # Columna L: Alcohol Peso
                row[12] = alcohol_volumen  # Columna M: Alcohol Volumen
                row[13] = peso_esp  # Columna N: Peso Esp
                row[14] = gaf  # Columna O: GAF
                
                new_rows.append(row)
                
            elif collection_name == 'tanque_presion':
                # TANQUE DE PRESIÓN - desde fila 5
                row = [''] * 20  # A-T (20 columnas)
                row[0] = doc.id  # Columna A: ID oculto
                row[1] = fecha  # Columna B: Fecha
                row[2] = data.get('Tipo (Ej: Trimalta )', '')  # Columna C: Tipo
                row[3] = data.get('N° Cocimiento (Ej:125-126)', '')  # Columna D: N° Cocimiento
                row[4] = data.get('Tp N° (Ej: 2)', '')  # Columna E: Tp N°
                row[5] = data.get('Tq N° (Ej: 9-7-6)', '')  # Columna F: Tq N°
                # Columna G vacía
                row[7] = data.get('Sedimentos (0/S/SS/SSS) (EJ: S)', '')  # Columna H: Sedimentos
                row[8] = data.get('Color [EBC] (Ej: 7.5)', '')  # Columna I: Color
                row[9] = data.get('Extrácto aparente [%] p/p (Ej: 2.06)', '')  # Columna J: Ext. Aparente
                row[10] = data.get('Volumen total [L] (Ej: 6650)', '')  # Columna K: Volumen total
                row[11] = data.get('Volumen H2O [L] (Ej: 1850)', '')  # Columna L: Volumen H2O
                row[12] = data.get('Tanque A (Ej: 1)', '')  # Columna M: Tanque A
                row[13] = data.get('Volumen total del Tanque A [L] (Ej: 2650)', '')  # Columna N: Volumen Tanque A
                row[14] = data.get('Tanque B (Ej: 14)', '')  # Columna O: Tanque B
                row[15] = data.get('Volumen total del Tanque B [L] (Ej: 1950)', '')  # Columna P: Volumen Tanque B
                row[16] = data.get('Tanque C (Ej: 9)', '')  # Columna Q: Tanque C
                row[17] = data.get('Volumen total del Tanque C [L] (Ej: 200)', '')  # Columna R: Volumen Tanque C
                row[18] = data.get('Observaciones', '')  # Columna S: Observaciones
                # Columna T vacía
                
                new_rows.append(row)
                
            elif collection_name == 'envasado':
                # ENVASADO - desde fila 6
                row = [''] * 15  # A-O (15 columnas)
                row[0] = doc.id  # Columna A: ID oculto
                row[1] = fecha  # Columna B: Fecha
                row[2] = data.get('Tipo (Ej: Occidental)', '')  # Columna C: Tipo
                row[3] = data.get('Calibre [ml] (Ej: 620)', '')  # Columna D: Calibre
                row[4] = data.get('Tq N° (Ej: 10-12)', '')  # Columna E: Tq N°
                row[5] = data.get('Tp N° (Ej: 1)', '')  # Columna F: Tp N°
                row[6] = data.get('N° Cocimiento (Ej: 91-92-95-96)', '')  # Columna G: N° Cocimiento
                row[7] = data.get('Turbidez [EBC] (Ej: 0.3)', '')  # Columna H: Turbidez
                row[8] = data.get('Degustación (OK ; no OK)', '')  # Columna I: Degustación
                row[9] = data.get('Sedimentos (0/S/SS/SSS) (Ej: 0)', '')  # Columna J: Sedimentos
                row[10] = data.get('T set [°C] (Pasteurizadora) (Ej: 69)', '')  # Columna K: T set
                row[11] = data.get('T max [°C] (Pasteurizadora) (Ej: 69.1)', '')  # Columna L: T max
                row[12] = data.get('UP', '')  # Columna M: UP
                row[13] = data.get('NaOH [%] (Lavadora) (Ej: 0.48)', '')  # Columna N: NaOH
                row[14] = data.get('Observaciones (Ej: Adición 1/2 bolsa soda)', '')  # Columna O: Observaciones
                
                new_rows.append(row)
                
            elif collection_name == 'producto_terminado':
                # PRODUCTO TERMINADO - desde fila 5
                row = [''] * 15  # A-O (15 columnas)
                row[0] = doc.id  # Columna A: ID oculto
                row[1] = fecha  # Columna B: Fecha
                row[2] = data.get('Código (Envasado/Vencimiento) (Ej: L = 150-00438 / V = 30-5-26)', '')  # Columna C: Código
                row[3] = data.get('Tipo (Ej: Trimalta Quinua)', '')  # Columna D: Tipo
                row[4] = data.get('Calibre [ml] (Ej: 300)', '')  # Columna E: Calibre
                row[5] = data.get('Tq N° (Ej: 1-10)', '')  # Columna F: Tq N°
                row[6] = data.get('Tp N° (Ej: 2)', '')  # Columna G: Tp N°
                row[7] = data.get('N° Cocimiento (Ej: 63-64)', '')  # Columna H: N° Cocimiento
                row[8] = data.get('pH (Ej: 4.67)', '')  # Columna I: pH
                row[9] = data.get('Color [EBC] (Ej: 150)', '')  # Columna J: Color
                row[10] = data.get('Extrácto aparente [%] p/p (Ej: 11.2)', '')  # Columna K: Ext. Aparente
                row[11] = data.get('Espuma [seg] (123)', '')  # Columna L: Espuma
                row[12] = data.get('Sedimentos 0°C (0/S/SS/SSS) (Ej: S)', '')  # Columna M: Sedimentos 0°C
                row[13] = data.get('Sedimentos 20°C (0/S/SS/SSS) (Ej: SS)', '')  # Columna N: Sedimentos 20°C
                row[14] = data.get('Observaciones', '')  # Columna O: Observaciones
                
                new_rows.append(row)
        
        # Escribir nuevos datos
        if new_rows:
            # Determinar la fila de inicio según la colección
            if collection_name == 'cocimiento' or collection_name == 'fermentacion' or collection_name == 'envasado':
                start_row = 6
            else:  # tanque_presion y producto_terminado
                start_row = 5
                
            # Buscar la última fila con datos
            existing_data = worksheet.get_all_values()
            last_row = start_row
            
            # Buscar la primera fila VACÍA después de los datos existentes
            for i in range(start_row-1, len(existing_data)):
               if not existing_data[i]:  # Si la fila está VACÍA
                   last_row = i + 1
                   break
            else:
                # Si no encontró filas vacías, usar la siguiente después de la última
                last_row = len(existing_data) + 1
            
            # Determinar el rango de columnas según la colección
            if collection_name == 'cocimiento':
                end_col = 'M'
            elif collection_name == 'fermentacion':
                end_col = 'O'  # Ahora va hasta la columna O por los cálculos
            elif collection_name == 'tanque_presion':
                end_col = 'T'
            elif collection_name == 'envasado':
                end_col = 'O'
            elif collection_name == 'producto_terminado':
                end_col = 'O'
            
            # Escribir datos desde la última fila vacía
            for i, row in enumerate(new_rows):
                range_start = f'A{last_row + i}'
                range_end = f'{end_col}{last_row + i}'
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
                
                # Obtener IDs existentes (columna A)
                existing_data = worksheet.get_all_values()
                existing_ids = set()
                
                # Determinar la fila de inicio según la colección
                if collection_name == 'cocimiento' or collection_name == 'fermentacion' or collection_name == 'envasado':
                    start_idx = 5  # Saltar hasta la fila 5 (las primeras 5 filas son encabezados)
                else:  # tanque_presion y producto_terminado
                    start_idx = 4  # Saltar hasta la fila 4 (las primeras 4 filas son encabezados)
                
                if len(existing_data) > start_idx:
                    for row in existing_data[start_idx:]:
                        if row and row[0]:  # Columna A tiene el ID
                            existing_ids.add(row[0])
                
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
