import pandas as pd
import os
import sys

#  CONFIGURAÇÕES DE CAMINHO

PASTA_ORIGEM = r"C:\Users\isado\OneDrive\Área de Trabalho\archives\dados_brutos"

# Arquivos LIMPOS

PASTA_DESTINO = r"C:\Users\isado\OneDrive\Área de Trabalho\archives\Trusted"


# 1. CLASSE BASE 

class OlistBaseETL:
    def __init__(self, caminho_entrada, caminho_saida):
        self.caminho_entrada = caminho_entrada
        self.caminho_saida = caminho_saida
        self.nome_arquivo = os.path.basename(caminho_entrada)
        self.df = None

    def extract(self):
        """Lê o arquivo da pasta de origem."""
        try:
            print(f"[Lendo] {self.nome_arquivo}...")
            self.df = pd.read_csv(self.caminho_entrada)
            return True
        except FileNotFoundError:
            print(f"Arquivo não encontrado: {self.caminho_entrada}")
            return False
        except Exception as e:
            print(f"Erro ao ler: {e}")
            return False

    def transform(self):
        """Limpeza básica aplicada a TODOS os arquivos."""
        if self.df is not None:
            # 1. Remover duplicatas exatas
            self.df = self.df.drop_duplicates()
            
            # 2. Remover espaços em branco de textos (strip)
            cols_texto = self.df.select_dtypes(include=['object']).columns
            self.df[cols_texto] = self.df[cols_texto].apply(lambda x: x.str.strip())
            
        return self.df

    def load(self):
        """Salva o arquivo limpo na pasta de destino em PARQUET."""
        if self.df is not None:
            # Garante que a pasta existe
            os.makedirs(os.path.dirname(self.caminho_saida), exist_ok=True)
            
            # Salva em Parquet (preserva os tipos de data!)
            self.df.to_parquet(self.caminho_saida, index=False)
            print(f"[Salvo] {os.path.basename(self.caminho_saida)}")


# 2. CLASSES ESPECIALISTAS 


class OrdersCleaner(OlistBaseETL):
    def transform(self):
        super().transform() # Roda limpeza básica
        if self.df is not None:
            print("    Convertendo datas de Pedidos...")
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
            print("    Convertendo data limite de envio...")
            self.df['shipping_limit_date'] = pd.to_datetime(self.df['shipping_limit_date'], errors='coerce')
        return self.df

class ReviewsCleaner(OlistBaseETL):
    def transform(self):
        super().transform()
        if self.df is not None:
            print("    Limpando textos e datas de Reviews...")
            # Datas
            self.df['review_creation_date'] = pd.to_datetime(self.df['review_creation_date'], errors='coerce')
            self.df['review_answer_timestamp'] = pd.to_datetime(self.df['review_answer_timestamp'], errors='coerce')
            
            # Limpeza de Texto (Remover 'Enter' nos comentários)
            self.df['review_comment_message'] = self.df['review_comment_message'].str.replace('\n', ' ', regex=False)
            self.df['review_comment_message'] = self.df['review_comment_message'].str.replace('\r', ' ', regex=False)
        return self.df

class ProductsCleaner(OlistBaseETL):
    def transform(self):
        super().transform()
        if self.df is not None:
            print("    Tratando categorias nulas...")
            self.df['product_category_name'] = self.df['product_category_name'].fillna('outros')
        return self.df


# 3. ORQUESTRADOR 

MAPA_PIPELINE = {
    'olist_orders_dataset.csv': OrdersCleaner,
    'olist_order_items_dataset.csv': OrderItemsCleaner,
    'olist_order_reviews_dataset.csv': ReviewsCleaner,
    'olist_products_dataset.csv': ProductsCleaner,
    'olist_customers_dataset.csv': OlistBaseETL,
    'olist_geolocation_dataset.csv': OlistBaseETL,
    'olist_order_payments_dataset.csv': OlistBaseETL,
    'olist_sellers_dataset.csv': OlistBaseETL,
    'product_category_name_translation.csv': OlistBaseETL
}

def rodar_pipeline():
    print("INICIANDO PIPELINE DE DADOS OLIST\n")
    
    sucessos = 0
    falhas = 0

    for arquivo, ClasseETL in MAPA_PIPELINE.items():
        # Caminho de Entrada
        input_path = os.path.join(PASTA_ORIGEM, arquivo)
        
        # Caminho de Saída (AGORA COM EXTENSÃO .parquet CORRETA)
        nome_parquet = f"clean_{arquivo}".replace('.csv', '.parquet')
        output_path = os.path.join(PASTA_DESTINO, nome_parquet)
        
        # Instancia a classe
        etl = ClasseETL(input_path, output_path)
        
        # Executa
        if etl.extract():
            etl.transform()
            etl.load()
            sucessos += 1
        else:
            falhas += 1
        
        print("-" * 40)

    print(f"\nFIM DO PROCESSO.")
    print(f"Arquivos processados: {sucessos}")
    print(f"Verifique a pasta: {PASTA_DESTINO}")

if __name__ == "__main__":
    rodar_pipeline()