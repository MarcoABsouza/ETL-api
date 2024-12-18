import os
import time
import requests
import logging
import logfire
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from logging import basicConfig, getLogger

# Importar Base e BitcoinPreco do database.py
from database import Base, BitcoinPreco

# Configuração Logfire
logfire.configure()
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()



# Carrega variáveis de ambiente .env
load_dotenv()

# Lê as variáveis de ambiente do arquivo .env
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

# Monta a URL de conexão com o banco de dados
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Cria a engine do SQLAlchemy
engine = create_engine(DATABASE_URL)

# Cria a sessão do SQLAlchemy
Session = sessionmaker(bind=engine)

def cria_tabela():
    """Cria as tabelas no banco de dados, se não existirem."""
    Base.metadata.create_all(bind=engine)
    print("Tabelas criadas com sucesso.")

def extrair_dados_bitcoin():
    """Extrai o JSON completo da API da Coinbase"""
    url = "https://api.coinbase.com/v2/prices/spot"
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        print(f"Erro ao extrair dados da API: {resposta.status_code}")
        return None

def tratar_dados_bitcoin(dados_json):
    """Trata os dados JSON para extrair apenas o preço do Bitcoin"""
    valor = float(dados_json['data']['amount'])
    criptomoeda  = dados_json['data']['base']
    moeda = dados_json['data']['currency']
    timestamp = datetime.now()

    dados_tratados = {
        "valor_bitcoin": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp
    }

    return dados_tratados

def salvar_dados_postgres(dados_tratados):
    "Salva os dados no Banco de Dados PostgreSQL."
    session = Session()
    try:
        novo_registro = BitcoinPreco(**dados_tratados)
        session.add(novo_registro)
        session.commit()
        logger.info(f"[{dados_tratados['timestamp']}] Dados salvos no PostgreSQL!")
    except Exception as e:
        logger.error(f"Erro ao inserir dados no PostgreSQL: {e}")
        session.rollback()
    finally:
        session.close()

def pipeline_bitcoin():
    """Executa a pipeline de ETL do Bitcoin com spans do Logfire"""
    with logfire.span("Executando pipeline ETL Bitcoin"):
        with logfire.span("Extrair Dados da API Coinbase"):
            dados_json = extrair_dados_bitcoin()
        
        if not dados_json:
            logger.error("Falha na extração dos dados. Abortando pipeline.")
            return
        
        with logfire.span("Tratar Dados do Bitcoin"):
            dados_tratados = tratar_dados_bitcoin(dados_json)
        
        with logfire.span("Salvar Dados no Postgres"):
            salvar_dados_postgres(dados_tratados)

        # Exemplo de log final com placeholders
        logger.info(
            f"Pipeline finalizada com sucesso!"
        )

if __name__ == "__main__":
    cria_tabela()
    logger.info("Iniciando pipeline ETL com atualização a cada 15 segundos... (CTRL+C para interromper)")

    while True:
        try:
            pipeline_bitcoin()
            time.sleep(15)
        except KeyboardInterrupt:
            logger.info("Processo interrompido pelo usuário. Finalizando...")
            break
        except Exception as e:
            logger.error(f"Erro inesperado durante a pipeline: {e}")
            time.sleep(15)