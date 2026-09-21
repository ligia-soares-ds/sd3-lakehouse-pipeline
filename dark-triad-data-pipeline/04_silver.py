# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 — Camada Silver
# MAGIC
# MAGIC **Etapa 4.4 do MVP — pipeline (ETL)**
# MAGIC
# MAGIC Aqui aplico os tratamentos que decidi no notebook de qualidade e entrego uma tabela confiável, tipada e pronta para ser modelada.
# MAGIC
# MAGIC O que faço nesta camada:
# MAGIC
# MAGIC 1. converto os itens de texto para inteiro;
# MAGIC 2. excluo respondentes com qualquer item em branco (valor 0);
# MAGIC 3. excluo respondentes que marcaram o mesmo valor nos 27 itens;
# MAGIC 4. padronizo as colunas contextuais;
# MAGIC 5. gero um identificador de respondente.
# MAGIC
# MAGIC O que **não** faço aqui: inverter itens ou calcular escores. Essas operações pertencem ao modelo analítico e ficam na camada Gold, para que a Silver continue sendo uma versão limpa do dado original, sem interpretação embutida.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

itens = [f"{letra}{i}" for letra in ["M", "N", "P"] for i in range(1, 10)]

df = spark.table("dark_triad.bronze.sd3_raw")
total_inicial = df.count()
print("Registros na Bronze:", total_inicial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tipagem
# MAGIC
# MAGIC Na Bronze mantive tudo como texto para preservar o dado exatamente como chegou. Agora converto os 27 itens e a coluna `source` para inteiro, e aplico `trim` em `country` para eliminar espaços acidentais.

# COMMAND ----------

df_tipado = df.select(
    *[F.col(c).cast("int").alias(c) for c in itens],
    F.trim(F.col("country")).alias("country"),
    F.col("source").cast("int").alias("source"),
    F.col("_data_ingestao"),
)

display(df_tipado.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exclusão de respondentes com itens em branco
# MAGIC
# MAGIC Como a escala do SD3 vai de 1 a 5, o valor 0 significa item não respondido. Identifiquei 452 respondentes nessa condição (2,5% da amostra), a maioria com um único item faltante, mas alguns com o questionário quase todo — ou totalmente — vazio.
# MAGIC
# MAGIC Optei por excluir qualquer respondente com item em branco. Assim todos os escores são calculados sobre os mesmos 9 itens por traço, e evito imputar valores que eu não tenho como estimar de forma defensável.

# COMMAND ----------

qtd_zeros = sum([(F.col(c) == 0).cast("int") for c in itens])

df_sem_branco = df_tipado.withColumn("_qtd_zeros", qtd_zeros).filter(F.col("_qtd_zeros") == 0).drop("_qtd_zeros")

removidos_branco = total_inicial - df_sem_branco.count()
print(f"Removidos por item em branco: {removidos_branco}")
print(f"Restantes: {df_sem_branco.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exclusão de respostas uniformes
# MAGIC
# MAGIC Encontrei 31 respondentes que marcaram exatamente o mesmo valor nos 27 itens. Um padrão assim indica que a pessoa não discriminou entre as perguntas, o que compromete a validade individual da resposta — o instrumento inclui itens de conteúdos bem diferentes, e concordar igualmente com todos é implausível.
# MAGIC
# MAGIC Parte desses casos já sai na etapa anterior (os que responderam tudo 0). Os demais removo aqui.
# MAGIC
# MAGIC Vale registrar um achado que contraria a expectativa da literatura: entre os padrões uniformes, responder tudo no valor máximo foi o dobro de frequente do que responder tudo no mínimo. A discussão sobre desejabilidade social nessas escalas costuma enfatizar a negação dos traços, mas aqui o padrão inverso apareceu mais.

# COMMAND ----------

valores_distintos = F.size(F.array_distinct(F.array(*[F.col(c) for c in itens])))

df_limpo = df_sem_branco.withColumn("_distintos", valores_distintos).filter(F.col("_distintos") > 1).drop("_distintos")

removidos_uniforme = df_sem_branco.count() - df_limpo.count()
print(f"Removidos por resposta uniforme: {removidos_uniforme}")
print(f"Registros válidos: {df_limpo.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identificador de respondente
# MAGIC
# MAGIC O dataset original não traz identificador. Como vou modelar em esquema estrela, preciso de uma chave para ligar as tabelas fato e dimensão, então gero um `id_respondente` sequencial.
# MAGIC
# MAGIC Uma observação de método: por não haver identificador na origem, não é possível distinguir uma duplicata real de duas pessoas que responderam igual. Por isso não removi os 59 registros com respostas repetidas — todos eram padrões extremos, compatíveis com coincidência.

# COMMAND ----------

df_silver = df_limpo.withColumn(
    "id_respondente",
    F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

df_silver = df_silver.select("id_respondente", *itens, "country", "source", "_data_ingestao")

display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persistência e documentação
# MAGIC
# MAGIC Gravo a tabela em Delta e registro a descrição no Unity Catalog, para que o catálogo reflita o que cada camada contém.

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dark_triad.silver.sd3_respostas")
)

spark.sql("""
    COMMENT ON TABLE dark_triad.silver.sd3_respostas IS
    'Camada Silver: respostas ao SD3 tipadas e validadas. Exclui respondentes com itens em branco e com resposta uniforme nos 27 itens. Um registro por respondente, com identificador gerado no pipeline. Itens mantidos na pontuação original, sem inversão.'
""")

print("Tabela dark_triad.silver.sd3_respostas criada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência
# MAGIC
# MAGIC Confirmo que os itens agora variam apenas de 1 a 5 e registro quanto da amostra original foi preservado.

# COMMAND ----------

df_check = spark.table("dark_triad.silver.sd3_respostas")

minimos = df_check.select(F.least(*[F.min(c) for c in itens]).alias("menor_valor"),
                          F.greatest(*[F.max(c) for c in itens]).alias("maior_valor"))
display(minimos)

final = df_check.count()
print(f"Bronze: {total_inicial} | Silver: {final} | Retenção: {100 * final / total_inicial:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado desta etapa
# MAGIC
# MAGIC - Tabela `dark_triad.silver.sd3_respostas` com uma linha por respondente válido, tipada e com identificador.
# MAGIC - Cada exclusão documentada com sua justificativa.
# MAGIC
# MAGIC **Próximo notebook:** `05_gold` — construção das dimensões e fatos, inversão dos itens e cálculo dos escores e perfis.