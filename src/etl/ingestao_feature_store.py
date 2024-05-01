import datetime
import sqlalchemy
import os
import pandas as pd
import argparse
from tqdm import tqdm # Biblioteca para criar uma barra de carregamento



# Cria o range de datas para a criação das safras
def date_range(dt_start, dt_stop, period='daily'):

    date_start = datetime.datetime.strptime(dt_start, '%Y-%m-%d') # Obtém um datetime a partir de uma string
    date_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d') # Obtém um datetime a partir de uma string
    dates = []

    while date_start <= date_stop:
        dates.append(date_start.strftime('%Y-%m-%d'))
        date_start += datetime.timedelta(days=1)

    if period == 'daily':
        return dates
    elif period == 'monthly':
        return [i for i in dates if i.endswith('01')] # Retorna as datas que terminam com 01 (dias primeiros de cada mês)
     



# Classe para ingestão e criação das safras
class Ingestor:

    def __init__(self, path, table, key_field):
        self.path = path
        self.engine = self.create_db_engine()
        self.table = table
        self.key_field = key_field # Chave primária
    
    # Abre uma conexão com o banco de dados
    def create_db_engine(self):
        return sqlalchemy.create_engine(f'sqlite:///{self.path}')
    

    def import_query(self, path):
        with open(path, 'r') as open_file:
            return open_file.read()


    def table_exists(self):
        with self.engine.connect() as connection:
            tables = sqlalchemy.inspect(connection).get_table_names()
            return self.table in tables
    

    def execute_etl(self, query):
        with self.engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df


    def insert_table(self, df):
        with self.engine.connect() as connection:
            df.to_sql(self.table, connection, if_exists='append', index=False)
        return True


    def delete_table_rows(self, value):
        sql = f"DELETE FROM {self.table} WHERE {self.key_field} = '{value}';"
        with self.engine.connect() as connection:
            connection.execute(sqlalchemy.text(sql))
            connection.commit() # Para exclusão foi necessário o commit, na inclusão não (provavelmente devido ao uso da API do Pandas)
        return True


    def update_table(self, raw_query, value):
        if self.table_exists():
            # print('Removendo dados existentes...')
            self.delete_table_rows(value)
        
        # print('Executando ETL...')
        df = self.execute_etl(raw_query.format(date=value)) 

        # print('Salvando dados na tabela...')
        self.insert_table(df)



def main():

    # Paths
    ETL_DIR = os.path.dirname(os.path.abspath(__file__))
    SOURCE_DIR = os.path.dirname(ETL_DIR)
    ROOT_DIR = os.path.dirname(SOURCE_DIR)
    DATA_DIR = os.path.join(ROOT_DIR, 'data')
    DB_PATH = os.path.join(DATA_DIR, 'olist.db')

    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str)
    parser.add_argument("--dt_start", type=str)
    parser.add_argument("--dt_stop", type=str)
    parser.add_argument("--dt_period", type=str)
    args = parser.parse_args()

    # Chama a função date_range
    dates = date_range(args.dt_start, args.dt_stop, args.dt_period)

    # Instancia a classe Ingestor
    ingestor = Ingestor(DB_PATH, args.table, 'dtReference')
    query_path = os.path.join(ETL_DIR, f'{args.table}.sql')
    query = ingestor.import_query(query_path) 

    for i in tqdm(dates):
        ingestor.update_table(query, i)


if __name__ == '__main__':
    main()