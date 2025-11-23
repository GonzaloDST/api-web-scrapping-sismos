import json
import boto3
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re
import time

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['EARTHQUAKES_TABLE'])

class EarthquakeScraper:
    def __init__(self):
        self.base_url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def scrape_earthquakes(self):
        try:
            print("🔍 Iniciando scraping de sismos del IGP...")
            print(f"📡 URL: {self.base_url}")
            
            response = requests.get(self.base_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            print(f"✅ Página cargada correctamente. Status: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Guardar el HTML para debugging
            with open('/tmp/igp_page.html', 'w', encoding='utf-8') as f:
                f.write(soup.prettify())
            print("💾 HTML guardado en /tmp/igp_page.html para análisis")
            
            earthquakes = self.extract_earthquakes_from_html(soup)
            print(f"📊 Se encontraron {len(earthquakes)} sismos")
            
            return earthquakes
            
        except Exception as e:
            print(f"❌ Error en scraping: {str(e)}")
            import traceback
            print(f"🔍 Stack trace: {traceback.format_exc()}")
            return []
    
    def extract_earthquakes_from_html(self, soup):
        earthquakes = []
        
        # Método 1: Buscar por texto característico de sismos
        print("🔍 Buscando datos de sismos...")
        
        # Buscar tablas
        tables = soup.find_all('table')
        print(f"📋 Se encontraron {len(tables)} tablas")
        
        for i, table in enumerate(tables):
            print(f"🔍 Analizando tabla {i+1}...")
            table_earthquakes = self.extract_from_table(table)
            if table_earthquakes:
                earthquakes.extend(table_earthquakes)
                print(f"✅ Tabla {i+1}: {len(table_earthquakes)} sismos encontrados")
        
        # Método 2: Buscar por texto que contenga "magnitud", "profundidad", etc.
        if not earthquakes:
            print("🔍 Buscando por texto en la página...")
            text_earthquakes = self.extract_from_text(soup)
            earthquakes.extend(text_earthquakes)
        
        # Método 3: Buscar elementos div o sections que puedan contener datos
        if not earthquakes:
            print("🔍 Buscando en elementos div...")
            div_earthquakes = self.extract_from_divs(soup)
            earthquakes.extend(div_earthquakes)
        
        return earthquakes[:10]  # Máximo 10 sismos
    
    def extract_from_table(self, table):
        earthquakes = []
        try:
            rows = table.find_all('tr')
            print(f"📏 Tabla tiene {len(rows)} filas")
            
            for j, row in enumerate(rows):
                if j == 0:  # Saltar posible header
                    continue
                    
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:  # Mínimo 3 columnas esperadas
                    print(f"🔍 Procesando fila {j}: {len(cells)} celdas")
                    
                    # Imprimir contenido de celdas para debugging
                    for k, cell in enumerate(cells):
                        text = cell.get_text(strip=True)
                        if text:
                            print(f"   Celda {k}: '{text}'")
                    
                    earthquake = self.parse_earthquake_row(cells)
                    if earthquake:
                        earthquakes.append(earthquake)
                        print(f"✅ Sismo {len(earthquakes)} procesado: {earthquake['magnitud']}M")
                        
                if len(earthquakes) >= 10:
                    break
                    
        except Exception as e:
            print(f"❌ Error procesando tabla: {str(e)}")
            
        return earthquakes
    
    def parse_earthquake_row(self, cells):
        try:
            # Extraer texto de todas las celdas
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            print(f"🔍 Parseando celdas: {cell_texts}")
            
            # Buscar patrones característicos
            fecha_hora = self.find_datetime(cell_texts)
            magnitud = self.find_magnitude(cell_texts)
            profundidad = self.find_depth(cell_texts)
            ubicacion = self.find_location(cell_texts)
            
            if not all([fecha_hora, magnitud > 0]):
                print("❌ Datos insuficientes para crear sismo")
                return None
            
            # Generar ID único
            earthquake_id = self.generate_id(fecha_hora, magnitud, ubicacion)
            
            # Parsear coordenadas
            lat, lon = self.parse_coordinates_from_text(ubicacion)
            
            earthquake = {
                'id': earthquake_id,
                'timestamp': self.parse_timestamp(fecha_hora),
                'fecha_hora': fecha_hora,
                'magnitud': magnitud,
                'profundidad_km': profundidad,
                'ubicacion': ubicacion,
                'latitud': lat,
                'longitud': lon,
                'scraped_at': datetime.utcnow().isoformat(),
                'raw_data': cell_texts  # Para debugging
            }
            
            return earthquake
            
        except Exception as e:
            print(f"❌ Error parseando fila: {str(e)}")
            return None
    
    def find_datetime(self, texts):
        """Buscar fecha y hora en los textos"""
        for text in texts:
            # Patrones de fecha/hora comunes
            patterns = [
                r'\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}',
                r'\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{2}:\d{2}',
                r'\d{1,2}-\d{1,2}-\d{4} \d{1,2}:\d{2}:\d{2}',
                r'\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    return match.group(0)
        
        # Si no encuentra, usar fecha actual
        return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    
    def find_magnitude(self, texts):
        """Buscar magnitud en los textos"""
        for text in texts:
            # Buscar patrones como "4.5", "5.2 M", "M 4.5", etc.
            patterns = [
                r'([\d.]+)\s*[mM]',
                r'[mM]\s*([\d.]+)',
                r'magnitud\s*([\d.]+)',
                r'(\d+\.\d+)',  # Cualquier número decimal
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    try:
                        return float(match.group(1))
                    except:
                        continue
        
        return 0.0
    
    def find_depth(self, texts):
        """Buscar profundidad en los textos"""
        for text in texts:
            # Buscar patrones como "35 km", "profundidad 25 km", etc.
            patterns = [
                r'(\d+)\s*km',
                r'profundidad\s*(\d+)',
                r'(\d+)\s*kilómetros',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    try:
                        return float(match.group(1))
                    except:
                        continue
        
        return 0.0
    
    def find_location(self, texts):
        """Buscar ubicación en los textos"""
        for text in texts:
            # Buscar texto que parezca una ubicación
            if len(text) > 10 and any(keyword in text.lower() for keyword in ['km', 'al', 'de', 'sur', 'norte', 'este', 'oeste']):
                return text
        
        # Si no encuentra, usar el texto más largo (probablemente ubicación)
        long_texts = [t for t in texts if len(t) > 15]
        return long_texts[0] if long_texts else "Ubicación no especificada"
    
    def extract_from_text(self, soup):
        """Extraer sismos del texto general de la página"""
        earthquakes = []
        try:
            text = soup.get_text()
            
            # Buscar patrones de sismos en el texto
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if any(keyword in line.lower() for keyword in ['sismo', 'temblor', 'magnitud', 'profundidad']):
                    print(f"🔍 Línea con keywords: {line}")
                    # Aquí podrías agregar lógica para parsear esta línea
                    
        except Exception as e:
            print(f"❌ Error en extract_from_text: {str(e)}")
            
        return earthquakes
    
    def extract_from_divs(self, soup):
        """Extraer sismos de elementos div"""
        earthquakes = []
        try:
            # Buscar divs que puedan contener datos de sismos
            divs = soup.find_all('div', class_=re.compile(r'sismo|earthquake|temblor', re.I))
            for div in divs:
                text = div.get_text(strip=True)
                if text:
                    print(f"🔍 Div encontrado: {text[:100]}...")
                    
        except Exception as e:
            print(f"❌ Error en extract_from_divs: {str(e)}")
            
        return earthquakes
    
    def parse_coordinates_from_text(self, text):
        """Parsear coordenadas desde texto de ubicación"""
        try:
            # Buscar patrones de coordenadas
            lat_match = re.search(r'(\d+\.\d+)°?\s*[NS]?', text, re.IGNORECASE)
            lon_match = re.search(r'(\d+\.\d+)°?\s*[WE]?', text, re.IGNORECASE)
            
            lat = float(lat_match.group(1)) if lat_match else 0.0
            lon = float(lon_match.group(1)) if lon_match else 0.0
            
            # Ajustar por dirección
            if 'S' in text.upper():
                lat = -lat
            if 'W' in text.upper() or 'O' in text.upper():
                lon = -lon
                
            return lat, lon
            
        except:
            return 0.0, 0.0
    
    def parse_timestamp(self, text):
        try:
            formats = [
                '%d/%m/%Y %H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%d-%m-%Y %H:%M:%S',
                '%d/%m/%Y %H:%M',
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(text, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            return datetime.utcnow().isoformat()
        except:
            return datetime.utcnow().isoformat()
    
    def generate_id(self, fecha_hora, magnitud, ubicacion):
        unique_string = f"{fecha_hora}_{magnitud}_{ubicacion}"
        return hashlib.md5(unique_string.encode()).hexdigest()

def lambda_handler(event, context):
    try:
        scraper = EarthquakeScraper()
        earthquakes = scraper.scrape_earthquakes()
        
        saved_count = 0
        for earthquake in earthquakes:
            try:
                table.put_item(Item=earthquake)
                saved_count += 1
                print(f"💾 Sismo guardado: {earthquake['id']}")
            except Exception as e:
                print(f"❌ Error guardando sismo: {str(e)}")
                continue
        
        response_body = {
            'message': 'Scraping completado exitosamente',
            'sismos_procesados': len(earthquakes),
            'sismos_guardados': saved_count,
            'timestamp': datetime.utcnow().isoformat(),
            'debug_info': {
                'earthquakes_found': len(earthquakes),
                'earthquakes_saved': saved_count
            }
        }
        
        response = {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response_body, ensure_ascii=False)
        }
        
    except Exception as e:
        response = {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json', 
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'Error en el scraping: {str(e)}',
                'debug': 'Revisa los logs de CloudWatch para más detalles'
            })
        }
    
    return response
