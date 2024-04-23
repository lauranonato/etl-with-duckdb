import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

#download pasta 
def baixar_arquivos(url_pasta, diretorio):
    os.makedirs(diretorio, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio, quiet=False, use_cookies=False)

#listar os arquivos csv no diretório especificado
def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_arquivos = os.listdir(diretorio)
    for arquivo in todos_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    return arquivos_csv

#funcao para ler um arquivo csv e retornar um dataframe 
def ler_csv(caminho_arquivo):
    df_duckdb = duckdb.read_csv(caminho_arquivo)
    print(df_duckdb)
    print(type(df_duckdb))

    return df_duckdb

#transformacao 
def transformar(df):
    df_transformado = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    print(df_transformado)
    return df_transformado

#converter o duckdb em pandas e salvar o df no postgres
def salvar_dados_no_postgres(df, table):
    DATABASE_URL = os.getenv("DATABASE_URL") 
    engine = create_engine(DATABASE_URL)
    df.to_sql(tabela, con=engine, if_exists='append',index=False)

if __name__ == "__main__":
    url_path = 'https://drive.google.com/drive/folders/1an5ZVxDh82Fhs4ayXljsss2Uy00TVnKt'
    local_dir = '/home/laura/Documentos/bootcamp-aulas/duckdb/gdown_pasta'
    arquivo = '/home/laura/Documentos/bootcamp-aulas/duckdb/gdown_pasta/vendas_05_01_2024.csv'


    #baixar_arquivos(url_path, local_dir)
    lista_de_arquivos = listar_arquivos_csv(local_dir)
    for arquivo in lista_de_arquivos:
        df_duckdb = ler_csv(arquivo)
        df_duckdb_trans = transformar(df_duckdb)
        salvar_dados_no_postgres(df_duckdb_trans,"vendas_calculado")