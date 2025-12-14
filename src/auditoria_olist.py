import pandas as pd
import os
import logging


# CONFIGURAÇÃO DE LOGGING

def configurar_logging():
    """Configura logging para arquivo e console"""
    logger = logging.getLogger('auditoria')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Salvar em arquivo
    file_handler = logging.FileHandler('auditoria_qualidade.log', mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Mostrar na tela
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger

logger = configurar_logging()


# CAMINHO DOS ARQUIVOS


PASTA_TRUSTED = r"C:\Users\isado\OneDrive\Área de Trabalho\projeto_etl\Trusted"


# FUNÇÕES DE AUDITORIA POR TABELA


def auditar_orders():
    """Auditoria da tabela de Pedidos"""
    logger.info("\n" + "=" * 60)
    logger.info("[ORDERS] Iniciando auditoria da tabela de Pedidos")
    logger.info("=" * 60)
    
    try:
        # Carregar arquivo
        caminho = os.path.join(PASTA_TRUSTED, "clean_olist_orders_dataset.parquet")
        df = pd.read_parquet(caminho)
        logger.info(f" Arquivo carregado: {len(df)} linhas")
        
        # CHECK 1: Data está em formato datetime?
        if pd.api.types.is_datetime64_any_dtype(df['order_purchase_timestamp']):
            logger.info(" CHECK 1: Data em formato DATETIME (correto)")
        else:
            logger.warning(" CHECK 1: Data ainda está em TEXTO (erro!)")
        
        # CHECK 2: Entrega antes da compra? (impossível)
        viagem_tempo = df[df['order_delivered_customer_date'] < df['order_purchase_timestamp']]
        if len(viagem_tempo) == 0:
            logger.info(" CHECK 2: Nenhuma 'viagem no tempo' detectada")
        else:
            logger.warning(f" CHECK 2: {len(viagem_tempo)} pedidos entregues ANTES de serem comprados!")
        
        # CHECK 3: IDs de pedidos duplicados?
        duplicados = df['order_id'].duplicated().sum()
        if duplicados == 0:
            logger.info(" CHECK 3: Nenhum order_id duplicado")
        else:
            logger.warning(f" CHECK 3: {duplicados} order_ids duplicados encontrados!")
        
        # CHECK 4: Datas nulas?
        nulos = df['order_purchase_timestamp'].isnull().sum()
        if nulos == 0:
            logger.info(" CHECK 4: Nenhuma data nula")
        else:
            logger.warning(f" CHECK 4: {nulos} datas nulas encontradas!")
    
    except FileNotFoundError:
        logger.error(" Arquivo de orders não encontrado!")



def auditar_order_items():
    """Auditoria da tabela de Itens de Pedidos"""
    logger.info("\n" + "=" * 60)
    logger.info("[ORDER ITEMS] Iniciando auditoria de Itens de Pedidos")
    logger.info("=" * 60)
    
    try:
        # Carregar arquivo
        caminho = os.path.join(PASTA_TRUSTED, "clean_olist_order_items_dataset.parquet")
        df = pd.read_parquet(caminho)
        logger.info(f" Arquivo carregado: {len(df)} linhas")
        
        # CHECK 1: Preços zerados ou negativos?
        precos_ruins = df[df['price'] <= 0]
        if len(precos_ruins) == 0:
            logger.info(" CHECK 1: Todos os preços são maiores que zero")
        else:
            logger.warning(f" CHECK 1: {len(precos_ruins)} itens com preço zerado/negativo!")
        
        # CHECK 2: Frete negativo?
        frete_negativo = df[df['freight_value'] < 0]
        if len(frete_negativo) == 0:
            logger.info(" CHECK 2: Nenhum frete negativo")
        else:
            logger.warning(f" CHECK 2: {len(frete_negativo)} itens com frete negativo!")
        
        # CHECK 3: Qual é o preço médio?
        preco_medio = df['price'].mean()
        logger.info(f" CHECK 3: Preço médio dos itens: R$ {preco_medio:.2f}")
    
    except FileNotFoundError:
        logger.error(" Arquivo de order_items não encontrado!")



def auditar_products():
    """Auditoria da tabela de Produtos"""
    logger.info("\n" + "=" * 60)
    logger.info("[PRODUCTS] Iniciando auditoria de Produtos")
    logger.info("=" * 60)
    
    try:
        # Carregar arquivo
        caminho = os.path.join(PASTA_TRUSTED, "clean_olist_products_dataset.parquet")
        df = pd.read_parquet(caminho)
        logger.info(f" Arquivo carregado: {len(df)} linhas")
        
        # CHECK 1: Existem categorias nulas ainda?
        nulos = df['product_category_name'].isnull().sum()
        if nulos == 0:
            logger.info(" CHECK 1: Nenhuma categoria nula (Pipeline funcionou!)")
        else:
            logger.warning(f" CHECK 1: Ainda existem {nulos} produtos sem categoria!")
        
        # CHECK 2: Quantos produtos têm categoria 'outros'?
        outros = (df['product_category_name'] == 'outros').sum()
        logger.info(f" CHECK 2: {outros} produtos com categoria 'outros'")
        
        # CHECK 3: Quantas categorias diferentes existem?
        total_categorias = df['product_category_name'].nunique()
        logger.info(f" CHECK 3: Total de categorias distintas: {total_categorias}")
        
        # CHECK 4: Produtos duplicados?
        duplicados = df['product_id'].duplicated().sum()
        if duplicados == 0:
            logger.info(" CHECK 4: Nenhum product_id duplicado")
        else:
            logger.warning(f" CHECK 4: {duplicados} product_ids duplicados!")
    
    except FileNotFoundError:
        logger.error(" Arquivo de products não encontrado!")



def auditar_geolocation():
    """Auditoria da tabela de Geolocalização"""
    logger.info("\n" + "=" * 60)
    logger.info("[GEOLOCATION] Iniciando auditoria de Geolocalização")
    logger.info("=" * 60)
    
    try:
        # Carregar arquivo
        caminho = os.path.join(PASTA_TRUSTED, "clean_olist_geolocation_dataset.parquet")
        df = pd.read_parquet(caminho)
        logger.info(f" Arquivo carregado: {len(df)} linhas")
        
        # CHECK 1: Coordenadas nulas?
        lat_nula = df['geolocation_lat'].isnull().sum()
        lng_nula = df['geolocation_lng'].isnull().sum()
        
        if lat_nula == 0 and lng_nula == 0:
            logger.info(" CHECK 1: Todas as coordenadas preenchidas")
        else:
            logger.warning(f" CHECK 1: {lat_nula} latitudes nulas, {lng_nula} longitudes nulas!")
        
        # CHECK 2: Latitude está entre -90 e 90?
        lat_invalida = df[(df['geolocation_lat'] < -90) | (df['geolocation_lat'] > 90)]
        if len(lat_invalida) == 0:
            logger.info(" CHECK 2: Todas as latitudes dentro do intervalo [-90, 90]")
        else:
            logger.warning(f" CHECK 2: {len(lat_invalida)} latitudes inválidas!")
        
        # CHECK 3: Longitude está entre -180 e 180?
        lng_invalida = df[(df['geolocation_lng'] < -180) | (df['geolocation_lng'] > 180)]
        if len(lng_invalida) == 0:
            logger.info(" CHECK 3: Todas as longitudes dentro do intervalo [-180, 180]")
        else:
            logger.warning(f" CHECK 3: {len(lng_invalida)} longitudes inválidas!")
        
        # CHECK 4: Quantos CEPs únicos existem?
        ceps_unicos = df['geolocation_zip_code_prefix'].nunique()
        logger.info(f" CHECK 4: Total de CEPs únicos: {ceps_unicos}")
    
    except FileNotFoundError:
        logger.error(" Arquivo de geolocation não encontrado!")


# FUNÇÃO PRINCIPAL


def executar_auditoria():
    """Executa todas as auditorias em sequência"""
    logger.info("\n")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║" + " " * 15 + ">>> AUDITORIA DE QUALIDADE DE DADOS <<<" + " " * 15 + "║")
    logger.info("╚" + "=" * 68 + "╝")
    
    # Executar auditoria de cada tabela
    auditar_orders()
    auditar_order_items()
    auditar_products()
    auditar_geolocation()
    
    # Resumo final
    logger.info("\n" + "=" * 60)
    logger.info(">>> AUDITORIA CONCLUÍDA <<<")
    logger.info("=" * 60)
    logger.info(f"Log salvo em: auditoria_qualidade.log\n")

if __name__ == "__main__":
    executar_auditoria()