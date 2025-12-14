# 🛒 Olist ETL Pipeline

Pipeline de Engenharia de Dados para processamento e limpeza do dataset público de E-commerce da Olist.

## 📌 Objetivo

Transformar dados brutos (CSV) em uma camada de dados confiáveis (Trusted/Parquet) com qualidade garantida e observabilidade através de logs estruturados.

---

## 🏗️ Arquitetura

```
Dados Brutos (CSV)
    ↓
[EXTRACT] → Leitura com tratamento de erros
    ↓
[TRANSFORM] → Limpeza e conversão de tipos
    ↓
[LOAD] → Salvamento em Parquet
    ↓
Dados Confiáveis (Trusted)
    ↓
[AUDITORIA] → Validação de qualidade
```

---

## 📂 Estrutura do Projeto

```
projeto_etl/
├── src/pipeline.py              # Pipeline ETL principal
├── auditoria_olist.py        # Script de validação de dados
├── pipeline_olist.log            # Log de execução
├── auditoria_olist.log       # Log de auditoria
├── README.md                     # Este arquivo
├── .gitignore
│
├── dados_brutos/                 # Arquivos CSV originais
├── Trusted/                      # Dados limpos em Parquet
└── Derived/                      # Dados derivados (opcional)
```

---

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install pandas openpyxl
```

### Executar

1. **Pipeline de limpeza:**
```bash
python main_pipeline.py
```

2. **Auditoria de qualidade:**
```bash
python auditoria_qualidade.py
```

---

## 📊 Pipeline (pipeline.py)

Processa 9 tabelas com limpeza específica:

- **Orders:** Conversão de timestamps para datetime
- **Order Items:** Validação de preços e fretes
- **Reviews:** Limpeza de quebras de linha
- **Products:** Preenchimento de categorias nulas
- **Geolocation:** Remoção de duplicatas
- **Demais tabelas:** Limpeza padrão (duplicatas e espaços)

Saída: Arquivos `.parquet` em `Trusted/`

---

## ✅ Auditoria (auditoria_olist.py)

Valida os dados processados com checks por tabela:

- **Orders:** Tipos de dados, lógica de negócio, chaves primárias
- **Order Items:** Preços válidos, fretes não-negativos
- **Products:** Categorias nulas, integridade de dados
- **Geolocation:** Coordenadas válidas, limites geográficos

Saída: Relatório de checks em `auditoria_qualidade.log`

---

## 🛠️ Tecnologias

- Python 3.12+
- Pandas
- Logging (built-in)
- Parquet (Snappy compression)

---

## 📞 Contato

**Isadora**  
🔗 [LinkedIn](https://www.linkedin.com/in/isadorapbernards/)  
📧 isadora.bernardes74@hotmail.com