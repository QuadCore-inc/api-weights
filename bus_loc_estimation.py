from pymongo import MongoClient
import time
from datetime import datetime, timedelta
import math
import numpy as np
# Conexão com MongoDB
connection_string = "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["BusON_Crowdsourcing"]
collection = "buses_locations"

window_size = 60

import time
from datetime import datetime, timezone, timedelta


def calc_raw_weight(user, total_samples):
    """Calcula o peso bruto para um usuário."""
    weight_backward = 0.5  # Peso para backward
    weight_rssi = 0.2      # Peso para RSSI
    weight_samples = 0.3   # Peso para n_samples

    normalized_backward = 1 / (1 + np.exp(user['backward']))  # Normalização do backward
    normalized_rssi = 1 / (1 + abs(user['mean_rssi']))       # Normalização do RSSI
    normalized_n_samples = user['n_samples'] / total_samples # Normalização de n_samples

    raw_weight = (normalized_backward * weight_backward +
                  normalized_rssi * weight_rssi +
                  normalized_n_samples * weight_samples)
    return raw_weight

def estimate_bus_location(users_vectors):
    total_lat = 0
    total_lon = 0

    for user in users_vectors:
        weight = user['weight']  # Peso já normalizado pela softmax
        total_lat += user['last_location_update']['latitude'] * weight
        total_lon += user['last_location_update']['longitude'] * weight

    # Como os pesos já somam 1, não precisamos dividir por total_weight
    estimated_lat = total_lat
    estimated_lon = total_lon

    return estimated_lat, estimated_lon

def softmax(values):
    """Aplica a função softmax a um vetor de valores."""
    exp_values = np.exp(values - np.max(values))  # Subtrai o máximo para evitar overflow
    return exp_values / np.sum(exp_values)

def calculate_and_estimate_bus_location(users_vectors):
    total_samples = sum([user['n_samples'] for user in users_vectors])  # Soma total de amostras
    
    # Passo 1: Calcular os raw_weight para cada usuário
    raw_weights = []
    for user in users_vectors:
        raw_weight = calc_raw_weight(user, total_samples)  # Calcula o peso bruto
        raw_weights.append(raw_weight)
        # print(f"User {user['user_id']} raw weight: {raw_weight}")
    
    # Passo 2: Aplicar a função softmax para normalizar os pesos
    normalized_weights = softmax(raw_weights)
    
    # Passo 3: Atribuir os pesos normalizados aos usuários
    for i, user in enumerate(users_vectors):
        user['weight'] = normalized_weights[i]
        print(f"User {user['user_id']} normalized weight: {user['weight']}")
    
    # Passo 4: Estimar a localização do ônibus usando os pesos normalizados
    estimated_lat, estimated_lon = estimate_bus_location(users_vectors)
    
    # Passo 5: Calcular a velocidade média e a direção média
    mean_speed = np.mean([user['last_location_update']['speed'] for user in users_vectors])
    mean_heading = np.mean([user['last_location_update']['heading'] for user in users_vectors])
    
    # Passo 6: Encontrar o tempo mais recente
    most_recent_time = max([user['last_update_time'] for user in users_vectors])
    
    return estimated_lat, estimated_lon, mean_speed, mean_heading, most_recent_time
        

def seach_and_filter_moviments(): 
    """Query movimentations in time window"""
    now = time.time()  
    current_window = now - window_size  
    
    collections = [col for col in db.list_collection_names() if '/' in col]
    
    for collection in collections: 
        if db[collection].count_documents({}) > 0:
            print(f"📌 Processing collection: {collection}")
            # Users documents; if the collection is empty, documents will be
            documents = db[collection].find({})
            if documents:
                users_vectors = []  
                for doc in documents: 
                    user_id = doc.get("_id")
                    last_update = doc.get("last_update")
                    if "time" in last_update:
                            try:
                                # Converter string de tempo para timestamp
                                dt = datetime.strptime(last_update["time"], "%Y-%m-%d %H:%M:%S %z")
                                last_updatetime = dt.timestamp()  # Converte para epoch time
                                if last_updatetime >= current_window: pass
                            except ValueError as e:
                                print(f"⚠ Erro ao converter tempo da última atualização para user {user_id}: {e}")
                                
                    movimentacoes = doc.get("user_movimentation", {}).values()
                    recent_movements = []
                    # Filtrar movimentações convertendo "time" corretamente
                    for mov in movimentacoes:
                        if "time" in mov:
                            try:
                                # Converter string de tempo para timestamp
                                dt = datetime.strptime(mov["time"], "%Y-%m-%d %H:%M:%S %z")
                                mov_time = dt.timestamp()  # Converte para epoch time
                                if mov_time >= current_window:
                                    recent_movements.append(mov)
                            except ValueError as e:
                                print(f"⚠ Erro ao converter time para user {user_id}: {e}")
                    
                    if recent_movements:
                        # User's id
                        user_vector = {'user_id': user_id}
                        # User's last update time
                        last_update_time = datetime.strptime(recent_movements[-1]['time'], "%Y-%m-%d %H:%M:%S %z")
                        user_vector['last_update_time'] = last_update_time.strftime("%Y-%m-%d %H:%M:%S %z")
                        user_vector['backward'] = last_update_time.timestamp() - now
                        # User's number of samples
                        n_samples = len(recent_movements)
                        user_vector['n_samples'] = n_samples
                        # User's mean RSSI
                        rssi_list = [movement['RSSI'] for movement in recent_movements if 'RSSI' in movement]
                        mean_rssi = np.mean(rssi_list)
                        mean_rssi = mean_rssi if mean_rssi != 0 else 1e-6
                        rssi_factor = 1 + (1/abs(mean_rssi))
                        user_vector['mean_rssi'] = float(mean_rssi)
                        user_vector['rssi_factor'] = float(rssi_factor)
                        # User's last location update
                        user_vector['last_location_update'] = {
                            "latitude": recent_movements[-1]['latitude'],
                            "longitude": recent_movements[-1]['longitude'],
                            "speed": recent_movements[-1]['speed'],
                            "heading": recent_movements[-1]['heading'],
                        }
                        
                        
                        users_vectors.append(user_vector)
                        
                        print(f"🚌 User {user_id} has {len(recent_movements)} recent updates. Last update in {user_vector['last_update_time']}")
                if len(users_vectors) > 0:
                    bus_lat, bus_long, bus_speed, bus_heading, bus_time = calculate_and_estimate_bus_location(users_vectors)
                    print(f"Localização estimatada do ônibus: Latitude {bus_lat} Longitude: {bus_long}")
                    create_or_update_bus(bus_collection=collection, 
                                     latitude=bus_lat,
                                     longitude=bus_long, 
                                     speed= bus_speed,
                                     heading=bus_heading,
                                     timestamp=bus_time
                                     )
                else: 
                    print("No recent users updates found!")
                
# Função para criar ou atualizar a localização do ônibus
def create_or_update_bus(bus_collection, latitude, longitude, speed, heading, timestamp):
    buses_locations_collection = db["buses_locations"]
    
    # Recuperando o documento com o nome da coleção (o ssid do ônibus)
    existing_bus = buses_locations_collection.find_one({"_id": bus_collection})
    splited_bus_ssid = bus_collection.split("/")
    
    bus_line = splited_bus_ssid[0]
    bus_id = splited_bus_ssid[1]
    # Dados do movimento
    frame_data = {
        "time": timestamp,
        "latitude": latitude,
        "longitude": longitude,
        "speed": speed,  # Substituído 'velocidade' por 'speed'
        "heading": heading,  # Adicionando o campo 'heading'
    }

    if not existing_bus:
        # Caso o documento não exista, cria um novo
        buses_locations_collection.insert_one({
            "_id": bus_collection,
            "bus_line": bus_line,
            "bus_id": bus_id,
            "last_update": {
                "time": timestamp,
                "latitude": latitude,
                "longitude": longitude,
                "speed": speed,  
                "heading": heading,  
            },
            "bus_movimentation": {
                "time_frame_1": frame_data
            }
        })
    else:
        # Caso o documento já exista, atualiza a localização
        movement_key = f"time_frame_{len(existing_bus['bus_movimentation']) + 1}"

        buses_locations_collection.update_one(
            {"_id": bus_collection},
            {
                "$set": {
                    "last_update": {
                        "time": timestamp,
                        "latitude": latitude,
                        "longitude": longitude,
                        "speed": speed,  
                        "heading": heading, 
                        
                    },
                    f"bus_movimentation.{movement_key}": frame_data
                }
            }
        )

# Função para remover o usuário (se necessário)
def remove_bus(bus_collection):
    db[collection].delete_one({"_id": bus_collection})
        
def loop_query(): 
    """Periodic Query"""
    while True: 
        seach_and_filter_moviments()
        time.sleep(5)
if __name__ == "__main__":
    loop_query()
