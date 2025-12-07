import pandas as pd
import os

# Ajuste para onde você salvou os Parquets
PASTA_TRUSTED = r"C:\Users\isado\OneDrive\Área de Trabalho\archives\Trusted"

def auditar_qualidade():
    print("INICIANDO AUDITORIA DE DADOS (DATA QUALITY)\n")
    
    
    # 1. AUDITORIA DE PEDIDOS (ORDERS)
    
    try:
        df_orders = pd.read_parquet(os.path.join(PASTA_TRUSTED, "clean_olist_orders_dataset.parquet"))
        print("[ORDERS] Verificando Pedidos...")

        # CHECK 1: Tipagem de Data
        if pd.api.types.is_datetime64_any_dtype(df_orders['order_purchase_timestamp']):
            print("    [Tipagem] Coluna de data está correta (datetime).")
        else:
            print("    [Tipagem] ERRO: Coluna de data ainda é texto!")

        # CHECK 2: Viagem no Tempo (Entrega antes da Compra?)
        # Filtra casos onde a entrega foi ANTES da compra (impossível)
        viagem_tempo = df_orders[df_orders['order_delivered_customer_date'] < df_orders['order_purchase_timestamp']]
        if len(viagem_tempo) == 0:
            print("    [Lógica] Nenhuma 'viagem no tempo' detectada.")
        else:
            print(f" [Lógica] ALERTA: {len(viagem_tempo)} pedidos entregues antes de serem comprados!")

        # CHECK 3: Duplicidade de Chave Primária
        if df_orders['order_id'].is_unique:
            print("    [Unicidade] Não existem order_id duplicados.")
        else:
            print("    [Unicidade] ERRO: Existem IDs de pedidos repetidos!")

    except FileNotFoundError:
        print("    Arquivo de orders não encontrado.")

    print("-" * 40)

    
    # 2. AUDITORIA DE ITENS (ORDER ITEMS)
    
    try:
        df_items = pd.read_parquet(os.path.join(PASTA_TRUSTED, "clean_olist_order_items_dataset.parquet"))
        print(" [ITEMS] Verificando Itens...")

        # CHECK 1: Preços Negativos ou Zero
        precos_estranhos = df_items[df_items['price'] <= 0]
        if len(precos_estranhos) == 0:
            print("    [Negócio] Todos os preços são maiores que zero.")
        else:
            print(f"    [Negócio] ALERTA: {len(precos_estranhos)} itens com preço zerado ou negativo.")
        
        # CHECK 2: Frete Negativo
        frete_negativo = df_items[df_items['freight_value'] < 0]
        if len(frete_negativo) == 0:
            print("    [Negócio] Nenhum frete negativo.")
        else:
             print(f"    [Negócio] ALERTA: {len(frete_negativo)} itens com frete negativo.")

    except FileNotFoundError:
        print("    Arquivo de items não encontrado.")

    print("-" * 40)

    
    # 3. AUDITORIA DE PRODUTOS (PRODUCTS)
    
    try:
        df_products = pd.read_parquet(os.path.join(PASTA_TRUSTED, "clean_olist_products_dataset.parquet"))
        print(" [PRODUCTS] Verificando Produtos...")

        # CHECK 1: Categorias Nulas (Lembra que preenchemos com 'outros'?)
        nulos = df_products['product_category_name'].isnull().sum()
        if nulos == 0:
            print("    [Limpeza] Nenhuma categoria nula encontrada (Pipeline funcionou!).")
        else:
            print(f"    [Limpeza] FALHA: Ainda existem {nulos} produtos sem categoria.")

    except FileNotFoundError:
        print("    Arquivo de produtos não encontrado.")
    
    print("\n FIM DA AUDITORIA.")

# Rodar
auditar_qualidade()