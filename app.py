from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import requests

app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("raw-data")

# Executar o script SQL para criar a tabela
execute_sql_script('sql/create_table.sql')

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.get_json()
    if not data or 'date' not in data or 'dados' not in data:
        return jsonify({"error": "Formato de dados inválido"}), 400

    try:
        datetime.fromtimestamp(data['date'])
        int(data['dados'])
    except (ValueError, TypeError):
        return jsonify({"error": "Tipo de dados inválido"}), 400

    # Processar e salvar dados
    filename = process_data(data)
    upload_file("raw-data", filename)

    # Ler arquivo Parquet do MinIO
    download_file("raw-data", filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    # Preparar e inserir dados no ClickHouse
    df_prepared = prepare_dataframe_for_insert(df_parquet)
    client = get_client()  # Obter o cliente ClickHouse
    insert_dataframe(client, 'working_data', df_prepared)

    return jsonify({"message": "Dados recebidos, armazenados e processados com sucesso"}), 200

@app.route('/upload_image', methods=['POST'])
def upload_image():
    if 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "Nenhum arquivo selecionado"}), 400

    # Atualize para a URL da API com a versão
    api_url = 'https://random-d.uk/api/v2/add?format=json'

    response = requests.post(
        api_url,
        files={'file': (file.filename, file)}
    )

    if response.status_code == 200:
        return jsonify(response.json()), 200
    else:
        return jsonify({"error": "Falha no upload da imagem", "status_code": response.status_code}), response.status_code

@app.route('/pokemon_data', methods=['POST'])
def pokemon_data():
    data = request.get_json()
    if not data or 'pokemon_id' not in data:
        return jsonify({"error": "Formato de dados inválido"}), 400

    pokemon_id = data['pokemon_id']

    try:
        # Fazer a requisição para a PokeAPI
        url = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}/'
        response = requests.get(url)

        if response.status_code == 200:
            pokemon_data = response.json()
            filename = process_data({
                "name": pokemon_data['name'],
                "height": pokemon_data['height'],
                "weight": pokemon_data['weight'],
                "abilities": [ability['ability']['name'] for ability in pokemon_data['abilities']]
            })

            # Carregar e processar o arquivo Parquet
            download_file("raw-data", filename, f"downloaded_{filename}")
            df_parquet = pd.read_parquet(f"downloaded_{filename}")
            df_prepared = prepare_dataframe_for_insert(df_parquet)
            
            # Inserir dados no ClickHouse
            client = get_client()
            insert_dataframe(client, 'pokemon_data', df_prepared)

            return jsonify({"message": "Dados do Pokémon recebidos, processados e armazenados com sucesso"}), 200
        else:
            return jsonify({"error": "Pokémon não encontrado", "status_code": response.status_code}), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
