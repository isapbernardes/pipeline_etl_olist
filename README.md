# 🛒 Olist ETL Pipeline: Engenharia de Dados com Python & POO


## 📌 Sobre o Projeto

Este projeto é um pipeline de **Engenharia de Dados (ETL)** desenvolvido para processar o dataset público de E-commerce da **Olist**.

O objetivo principal foi transformar dados brutos (CSV) em uma camada de dados confiáveis (Trusted/Parquet), garantindo consistência de tipagem, tratamento de nulos e qualidade de dados.

O diferencial técnico deste projeto é a aplicação de **Programação Orientada a Objetos (POO)** para criar um código modular, escalável e fácil de manter.

---

## 🏗️ Arquitetura e Solução

O pipeline segue o fluxo clássico **ETL (Extract, Transform, Load)**:

1.  **Extract (Extração):**
    * Leitura dinâmica de múltiplos arquivos CSV.
    * Tratamento de erros de leitura (`FileNotFound`).
    

2.  **Transform (Transformação):**
    * **Limpeza Genérica:** Remoção de duplicatas e espaços em branco (strip) aplicada a todos os arquivos via Herança.
    * **Limpeza Especializada:** Classes filhas aplicam regras de negócio específicas (ex: conversão de datas em `Orders`, limpeza de quebras de linha em `Reviews`).
    * **Polimorfismo:** O orquestrador trata todos os arquivos da mesma forma, mas cada classe executa sua própria versão da transformação.

3.  **Load (Carregamento):**
    * Armazenamento dos dados processados em formato **Parquet**.
    * **Por que Parquet?** Para garantir que os tipos de dados (especialmente datas) sejam preservados corretamente e reduzir o tamanho dos arquivos.

4.  **Data Quality (Auditoria):**
    * Script de validação automática que verifica tipagem, unicidade de chaves primárias e consistência lógica (ex: Data de Entrega não pode ser anterior à Data de Compra).

---

## 📂 Estrutura do Projeto

```bash
projeto-olist/
│
├── dados_brutos/          # (CSV) Arquivos originais do Kaggle
├── Trusted/               # (Parquet) Dados limpos e processados (Inclusos neste repo)
│
├── src/
│   ├── pipeline_olist.py  # Código Principal (ETL)
│   └── auditoria_olist.py # Script de Teste de Qualidade
│
├── .gitignore             # Arquivos ignorados pelo Git
├── README.md              # Documentação do projeto


## 💻 Tecnologias Utilizadas

* **Python 3.12**
* **Pandas:** Manipulação e tratamento de dados.
* **PyArrow/FastParquet:** Motores para gravação de arquivos Parquet.
* **OS/Sys:** Manipulação de sistema de arquivos e encoding.

---

## 🧠 Aprendizados e Conceitos Aplicados

Durante o desenvolvimento, foram aplicados conceitos fundamentais de Engenharia de Dados e Software:

* **Herança:** Criação de uma classe base `OlistBaseETL` para evitar repetição de código.
* **Encapsulamento:** As regras de limpeza de cada tabela ficam isoladas em suas próprias classes.
* **Armazenamento Otimizado:** Transição de CSV para Parquet para preservação de schema e redução de tamanho.
* **Data Quality:** Implementação de testes automatizados para garantir a confiança e integridade nos dados.

---

## 📞 Contato

**Isadora** 🔗 [LinkedIn](https://www.linkedin.com/in/isadorapbernards/)  
📧 [isadora.bernardes74@hotmail.com](mailto:isadora.bernardes74@hotmail.com)

