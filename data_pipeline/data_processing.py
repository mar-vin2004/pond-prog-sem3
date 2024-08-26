import pandas as pd
from datetime import datetime

def process_data(pokemon_data):
    # Criar DataFrame e salvar como Parquet
    df = pd.DataFrame([pokemon_data])
    filename = f"pokemon_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    df.to_parquet(filename)
    return filename

def prepare_dataframe_for_insert(df):
    df['data_ingestao'] = datetime.now()
    df['dado_linha'] = df.apply(lambda row: row.to_json(), axis=1)
    df['tag'] = 'pokemon_data'
    return df[['data_ingestao', 'dado_linha', 'tag']]
