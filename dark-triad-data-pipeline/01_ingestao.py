# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 — Ingestão dos dados (SD3)
# MAGIC
# MAGIC **Etapa 4.2 do MVP — Coleta**
# MAGIC
# MAGIC Este notebook traz os dados brutos do *Short Dark Triad* (SD3) para dentro da nuvem.
# MAGIC
# MAGIC **Fonte:** Open-Source Psychometrics Project (openpsychometrics.org/_rawdata), acessado pelo espelho no Kaggle (`lucasgreenwell/short-dark-triad-responses`).
# MAGIC
# MAGIC **Por que automatizar a coleta?** Baixar o arquivo manualmente funcionaria, mas o download via API torna o pipeline **reprodutível**: qualquer pessoa executa este notebook e obtém exatamente os mesmos dados, sem passos manuais não documentados.
# MAGIC
# MAGIC **Fluxo:** Kaggle → armazenamento temporário do cluster → Volume do Unity Catalog (persistente).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalação da biblioteca
# MAGIC
# MAGIC `kagglehub` é a biblioteca oficial do Kaggle para download de datasets. O `restartPython()` reinicia o interpretador para que a biblioteca recém-instalada fique disponível nas células seguintes.

# COMMAND ----------

# MAGIC %pip install kagglehub
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Download do dataset
# MAGIC
# MAGIC O `kagglehub` baixa os arquivos para o disco local do cluster (armazenamento **efêmero**: some quando o cluster é reiniciado). Por isso, na etapa seguinte os arquivos são copiados para um local persistente.

# COMMAND ----------

import kagglehub
import os

path = kagglehub.dataset_download("lucasgreenwell/short-dark-triad-responses")

print("Diretório temporário:", path)
print("Arquivos baixados:", os.listdir(path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criação do catálogo e da camada Bronze
# MAGIC
# MAGIC Todo o projeto vive em um catálogo próprio, `dark_triad`, com um schema por camada da Arquitetura Medalhão (`bronze`, `silver`, `gold`). Isso mantém o MVP isolado do restante do workspace e torna a estrutura do pipeline visível já na navegação do Unity Catalog.
# MAGIC
# MAGIC A camada **Bronze** guarda o dado exatamente como veio da fonte — é o "cofre de evidências" do pipeline. Dentro dela, um **Volume** armazena os arquivos originais de forma persistente.

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dark_triad")

for camada in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS dark_triad.{camada}")

spark.sql("CREATE VOLUME IF NOT EXISTS dark_triad.bronze.raw_files")

print("Catálogo, schemas e Volume prontos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persistência dos arquivos no Volume
# MAGIC
# MAGIC Cópia dos arquivos do diretório temporário para o Volume. A partir daqui, os dados brutos ficam disponíveis de forma estável para as próximas etapas do pipeline.

# COMMAND ----------

import shutil

destino = "/Volumes/dark_triad/bronze/raw_files/sd3"
os.makedirs(destino, exist_ok=True)

for arquivo in os.listdir(path):
    origem_arquivo = os.path.join(path, arquivo)
    if os.path.isfile(origem_arquivo):
        shutil.copy(origem_arquivo, destino)

print("Arquivos persistidos no Volume:")
for arquivo in os.listdir(destino):
    tamanho_mb = os.path.getsize(os.path.join(destino, arquivo)) / (1024 * 1024)
    print(f" - {arquivo} ({tamanho_mb:.2f} MB)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspeção inicial
# MAGIC
# MAGIC Antes de qualquer transformação, uma olhada nas primeiras linhas do arquivo para confirmar o separador, o cabeçalho e o formato geral. Nenhum dado é alterado aqui.

# COMMAND ----------

arquivo_dados = os.path.join(destino, "data.csv")

with open(arquivo_dados, "r", encoding="utf-8", errors="replace") as f:
    for i, linha in enumerate(f):
        print(linha[:500])
        if i >= 2:
            break

# COMMAND ----------

import os
destino = "/Volumes/dark_triad/bronze/raw_files/sd3"
print(os.listdir(destino))

# COMMAND ----------

with open(os.path.join(destino, "codebook.txt"), "r", encoding="utf-8", errors="replace") as f:
    print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado desta etapa
# MAGIC
# MAGIC - Dados brutos coletados via API e persistidos em `/Volumes/dark_triad/bronze/raw_files/sd3`.
# MAGIC - Nenhuma transformação aplicada até aqui, preservando a rastreabilidade em relação à fonte original.
# MAGIC
# MAGIC **Próximo notebook:** `02_bronze`: leitura do arquivo, criação da tabela Delta bruta e adição de metadados de controle.