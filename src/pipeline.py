import pandas as pd
import os
import sys
import logging


# CONFIGURAÇÃO DE LOGGING

def configurar_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Handler Arquivo
    file_handler = logging.FileHandler('pipeline_olist.log', mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Handler Tela
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger

logger = configurar_logging()


# CONFIGURAÇÕES DE CAMINHO

PASTA_ORIGEM = r"C:\Users\isado\OneDrive\Área de Trabalho\projeto_etl\dados_brutos"
PASTA_DESTINO = r"C:\Users\isado\OneDrive\Área de Trabalho\projeto_etl\Trusted"


# 1. CLASSE BASE 

class OlistBaseETL:
    def __init__(self, caminho_entrada, caminho_saida):
        self.caminho_entrada = caminho_entrada
        self.caminho_saida = caminho_saida
        self.nome_arquivo = os.path.basename(caminho_entrada)
        self.df = None

    def extract(self):
        try:
            logger.info(f"Iniciando leitura: {self.nome_arquivo}...") 
            self.df = pd.read_csv(self.caminho_entrada)
            return True
        except FileNotFoundError:
            logger.error(f"Arquivo NÃO encontrado: {self.caminho_entrada}")
            return False
        except Exception as e:
            logger.error(f"Erro crítico ao ler {self.nome_arquivo}: {e}")
            return False

    def transform(self):
        if self.df is not None:
            qtd_antes = len(self.df)
            self.df = self.df.drop_duplicates()
            qtd_depois = len(self.df)
            
            if qtd_antes != qtd_depois:
                logger.warning(f"[{self.nome_arquivo}] Duplicatas exatas removidas: {qtd_antes - qtd_depois}")
            
            cols_texto = self.df.select_dtypes(include=['object']).columns
            self.df[cols_texto] = self.df[cols_texto].apply(lambda x: x.str.strip())
            
        return self.df

    def load(self):
        if self.df is not None:
            try:
                os.makedirs(os.path.dirname(self.caminho_saida), exist_ok=True)
                self.df.to_parquet(self.caminho_saida, index=False)
                logger.info(f"Sucesso: Arquivo salvo em {os.path.basename(self.caminho_saida)}")
            except Exception as e:
                logger.critical(f"Falha ao salvar o arquivo {self.nome_arquivo}: {e}")


# 2. CLASSES ESPECIALISTAS 

class OrdersCleaner(OlistBaseETL):
    def transform(self):
        super().transform() 
        if self.df is not None:
            logger.info(f"[{self.nome_arquivo}] Convertendo colunas de datas...")
            cols = ['order_purchase_timestamp', 'order_approved_at', 
                    'order_delivered_carrier_date', 'order_delivered_customer_date', 
                    'order_estimated_delivery_date']
            for col in cols:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        return self.df

class OrderItemsCleaner(OlistBaseETL):
    def transform(self):
        super().transform()
        if self.df is not None:
            logger.info(f"[{self.nome_arquivo}] Convertendo data limite de envio...")
            self.df['shipping_limit_date'] = pd.to_datetime(self.df['shipping_limit_date'], errors='coerce')
        return self.df

class ReviewsCleaner(OlistBaseETL):
    def transform(self):
        super().transform()
        if self.df is not None:
            logger.info(f"[{self.nome_arquivo}] Limpando textos e convertendo datas...")
            self.df['review_creation_date'] = pd.to_datetime(self.df['review_creation_date'], errors='coerce')
            self.df['review_answer_timestamp'] = pd.to_datetime(self.df['review_answer_timestamp'], errors='coerce')
            self.df['review_comment_message'] = self.df['review_comment_message'].str.replace('\n', ' ', regex=False)
            self.df['review_comment_message'] = self.df['review_comment_message'].str.replace('\r', ' ', regex=False)
        return self.df

class ProductsCleaner(OlistBaseETL):
    def transform(self):
        super().transform()
        if self.df is not None:
            logger.info(f"[{self.nome_arquivo}] Tratando categorias nulas...")
            self.df['product_category_name'] = self.df['product_category_name'].fillna('outros')
        return self.df

class GeolocationCleaner(OlistBaseETL):
    def transform(self):
        """
        Limpeza de dados de geolocalização mantendo granularidade máxima.
        
        Nota: Este ETL preserva todos os registros únicos de geolocalização.
        Para análises que requerem agregação por CEP (ex: centróide), 
        consulte a análise exploratória em 'analise_geolocation_centroide.ipynb'
        """
        super().transform()
        
        if self.df is not None:
            linhas_processadas = len(self.df)
            ceps_unicos = self.df['geolocation_zip_code_prefix'].nunique()
            
            logger.info(f"[{self.nome_arquivo}] Processamento concluido.")
            logger.info(f"  - Total de registros unicos: {linhas_processadas}")
            logger.info(f"  - Total de CEPs distintos: {ceps_unicos}")
            logger.info(f"  - Media de registros por CEP: {linhas_processadas / ceps_unicos:.2f}")
            
            # Validacao: Verificar integridade de coordenadas
            nulos_lat = self.df['geolocation_lat'].isnull().sum()
            nulos_lng = self.df['geolocation_lng'].isnull().sum()
            
            if nulos_lat > 0 or nulos_lng > 0:
                logger.warning(f"  ALERTA: Valores nulos encontrados: lat={nulos_lat}, lng={nulos_lng}")
            
        return self.df


# 3. ORQUESTRADOR 

MAPA_PIPELINE = {
    'olist_orders_dataset.csv': OrdersCleaner,
    'olist_order_items_dataset.csv': OrderItemsCleaner,
    'olist_order_reviews_dataset.csv': ReviewsCleaner,
    'olist_products_dataset.csv': ProductsCleaner,
    'olist_geolocation_dataset.csv': GeolocationCleaner, 
    'olist_customers_dataset.csv': OlistBaseETL,
    'olist_order_payments_dataset.csv': OlistBaseETL,
    'olist_sellers_dataset.csv': OlistBaseETL,
    'product_category_name_translation.csv': OlistBaseETL
}

def rodar_pipeline():
    logger.info(">>> INICIANDO PIPELINE DE DADOS OLIST <<<")
    
    sucessos = 0
    falhas = 0

    for arquivo, ClasseETL in MAPA_PIPELINE.items():
        input_path = os.path.join(PASTA_ORIGEM, arquivo)
        nome_parquet = f"clean_{arquivo}".replace('.csv', '.parquet')
        output_path = os.path.join(PASTA_DESTINO, nome_parquet)
        
        etl = ClasseETL(input_path, output_path)
        
        if etl.extract():
            etl.transform()
            etl.load()
            sucessos += 1
        else:
            falhas += 1
            logger.warning(f"Falha no arquivo {arquivo}")
        
        logger.info("-" * 60)

    logger.info(">>> FIM DO PROCESSO <<<")
    logger.info(f"Resumo: {sucessos} processados | {falhas} falhas.")
    logger.info(f"Pasta de Saída: {PASTA_DESTINO}")

if __name__ == "__main__":
    rodar_pipeline()