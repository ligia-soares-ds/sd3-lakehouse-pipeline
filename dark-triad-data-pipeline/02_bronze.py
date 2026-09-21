# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 — Camada Bronze
# MAGIC
# MAGIC **Etapa 4.4 do MVP — início do pipeline (ETL)**
# MAGIC
# MAGIC Aqui o arquivo bruto vira uma **tabela Delta**, sem nenhuma limpeza ou conversão de valores.
# MAGIC
# MAGIC **Por que não transformar nada ainda?** A camada Bronze funciona como um cofre de evidências: se algo der errado nas camadas seguintes, sempre é possível voltar ao dado exatamente como ele chegou. A única coisa acrescentada são **metadados de controle** (quando e de onde o dado foi carregado), que não alteram o conteúdo original.
# MAGIC
# MAGIC **Estrutura do arquivo:** 29 colunas separadas por tabulação: 27 itens do SD3 (`M1`–`M9` de maquiavelismo, `N1`–`N9` de narcisismo, `P1`–`P9` de psicopatia), mais `country` e `source`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura do arquivo bruto
# MAGIC
# MAGIC Duas decisões importantes nesta leitura:
# MAGIC
# MAGIC - **`sep="\t"`**: apesar da extensão, o arquivo é separado por tabulação.
# MAGIC - **`inferSchema=False`**: todas as colunas são lidas como texto. Isso é proposital, a tipagem correta é responsabilidade da camada Silver. Ler tudo como texto evita que o Spark descarte silenciosamente valores inesperados (como células vazias ou não numéricas) já na entrada.

# COMMAND ----------

caminho_arquivo = "/Volumes/dark_triad/bronze/raw_files/sd3/data.csv"

df_bronze = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .option("inferSchema", False)
    .csv(caminho_arquivo)
)

print("Linhas:", df_bronze.count())
print("Colunas:", len(df_bronze.columns))
display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Metadados de controle
# MAGIC
# MAGIC São acrescentadas duas colunas técnicas, prefixadas com `_` para diferenciá-las dos dados originais:
# MAGIC
# MAGIC - `_data_ingestao`: quando o registro entrou no Lakehouse;
# MAGIC - `_arquivo_origem`: de qual arquivo ele veio.
# MAGIC
# MAGIC Esses campos sustentam a **linhagem dos dados**: em uma nova carga no futuro, é possível saber o que veio de cada execução.

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (
    df_bronze
    .withColumn("_data_ingestao", F.current_timestamp())
    .withColumn("_arquivo_origem", F.lit(caminho_arquivo))
)

display(df_bronze.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Persistência como tabela Delta
# MAGIC
# MAGIC A gravação em formato **Delta** é o que transforma um arquivo solto em uma tabela do Lakehouse: passa a ter esquema definido, transações ACID e histórico de versões (*time travel*).
# MAGIC
# MAGIC O modo `overwrite` permite reexecutar o notebook do zero sem duplicar dados.

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dark_triad.bronze.sd3_raw")
)

print("Tabela dark_triad.bronze.sd3_raw criada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Documentação da tabela no Unity Catalog
# MAGIC
# MAGIC A descrição registrada aqui aparece no catálogo da plataforma, cumprindo parte da exigência de **Catálogo de Dados** do MVP.

# COMMAND ----------

spark.sql("""
    COMMENT ON TABLE dark_triad.bronze.sd3_raw IS
    'Camada Bronze: respostas brutas ao Short Dark Triad (SD3), coletadas pelo Open-Source Psychometrics Project e obtidas via API do Kaggle. Dados preservados como na origem (todas as colunas como texto), acrescidos de metadados de ingestão.'
""")

display(spark.sql("DESCRIBE EXTENDED dark_triad.bronze.sd3_raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado desta etapa
# MAGIC
# MAGIC - Tabela `dark_triad.bronze.sd3_raw` persistida em Delta, fiel ao arquivo original.
# MAGIC - Linhagem registrada por meio dos metadados de ingestão.
# MAGIC
# MAGIC **Próximo notebook:** `03_qualidade` — análise de completude, consistência, unicidade, acurácia e outliers sobre esta camada, antes de qualquer limpeza.