import json
import boto3
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['EARTHQUAKES_TABLE'])

class EarthquakeScraper:
    def __init__(self):
        self.base_url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def scrape_earthquakes(self):
        try:
            print("Iniciando scraping de sismos...")
            response = requests.get(self.base_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            earthquakes = []
            
            # Buscar tablas en la página - ajustar según la estructura real
            tables = soup.find_all('table')
            print(f"Se encontraron {len(tables)} tablas en la página")
            
            for table in tables:
                rows = table.find_all('tr')[1:11]  # Saltar header y tomar máximo 10
                for row in rows:
                    earthquake = self.parse_row(row)
                    if earthquake:
                        earthquakes.append(earthquake)
                
                if earthquakes:  # Si encontramos datos en esta tabla, salir
                    break
            
            return earthquakes[:10]  # Retornar máximo 10 sismos
            
        except Exception as e:
            print(f"Error en scraping: {str(e)}")
            return []
    
    def parse_row(self, row):
        try:
            cells = row.find_all('td')
            if len(cells) < 4:
                return None
            
            # Extraer datos básicos (ajustar índices según la estructura real)
            fecha_hora = cells[0].get_text(strip=True)
            magnitud_text = cells[1].get_text(strip=True)
            profundidad_text = cells[2].get_text(strip=True)
            ubicacion = cells[3].get_text(strip=True)
            
            # Parsear magnitud
            magnitud = self.parse_magnitude(magnitud_text)
            
            # Parsear profundidad
            profundidad = self.parse_depth(profundidad_text)
            
            # Parsear coordenadas desde la ubicación
            lat, lon = self.parse_coordinates(ubicacion)
            
            # Generar ID único
            earthquake_id = self.generate_id(fecha_hora, lat, lon)
            
            # Crear timestamp
            timestamp = self.parse_timestamp(fecha_hora)
            
            earthquake = {
                'id': earthquake_id,
                'timestamp': timestamp,
                'fecha_hora': fecha_hora,
                'magnitud': magnitud,
                'profundidad_km': profundidad,
                'ubicacion': ubicacion,
                'latitud': lat,
                'longitud': lon,
                'scraped_at': datetime.utcnow().isoformat()
            }
            
            print(f"Sismo procesado: {earthquake_id}")
            return earthquake
            
        except Exception as e:
            print(f"Error parseando fila: {str(e)}")
            return None
    
    def parse_magnitude(self, text):
        try:
            # Buscar números como "4.5", "5.2 M", etc.
            match = re.search(r'(\d+\.\d+|\d+)', text)
            return float(match.group(1)) if match else 0.0
        except:
            return 0.0
    
    def parse_depth(self, text):
        try:
            # Buscar profundidad en km
            match = re.search(r'(\d+\.\d+|\d+)\s*km', text, re.IGNORECASE)
            return float(match.group(1)) if match else 0.0
        except:
            return 0.0
    
    def parse_coordinates(self, text):
        try:
            # Buscar patrones de coordenadas
            # Ejemplo: "12.5°S, 76.8°W" o "12.5 S, 76.8 W"
            lat_match = re.search(r'(\d+\.\d+)\s*°?\s*([NS])', text, re.IGNORECASE)
            lon_match = re.search(r'(\d+\.\d+)\s*°?\s*([WE])', text, re.IGNORECASE)
            
            if lat_match and lon_match:
                lat_val = float(lat_match.group(1))
                lat_dir = lat_match.group(2).upper()
                lon_val = float(lon_match.group(1))
                lon_dir = lon_match.group(2).upper()
                
                lat = lat_val * (-1 if lat_dir == 'S' else 1)
                lon = lon_val * (-1 if lon_dir == 'W' else 1)
                
                return lat, lon
            
            return 0.0, 0.0
        except:
            return 0.0, 0.0
    
    def parse_timestamp(self, text):
        try:
            # Intentar diferentes formatos de fecha
            formats = [
                '%d/%m/%Y %H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%d-%m-%Y %H:%M:%S',
                '%d/%m/%Y %H:%M',
                '%Y-%m-%d %H:%M'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(text, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # Si no coincide ningún formato, usar fecha actual
            return datetime.utcnow().isoformat()
        except:
            return datetime.utcnow().isoformat()
    
    def generate_id(self, fecha_hora, lat, lon):
        unique_string = f"{fecha_hora}_{lat}_{lon}"
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
                print(f"Sismo guardado: {earthquake['id']}")
            except Exception as e:
                print(f"Error guardando sismo: {str(e)}")
                continue
        
        response = {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': f'Scraping completado exitosamente',
                'sismos_procesados': len(earthquakes),
                'sismos_guardados': saved_count,
                'timestamp': datetime.utcnow().isoformat()
            })
        }
        
    except Exception as e:
        response = {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json', 
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'Error en el scraping: {str(e)}'
            })
        }
    
    return response