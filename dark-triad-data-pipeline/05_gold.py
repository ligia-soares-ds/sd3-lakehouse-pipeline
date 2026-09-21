# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 — Camada Gold
# MAGIC
# MAGIC **Etapas 4.3 e 4.4 do MVP — modelagem e pipeline**
# MAGIC
# MAGIC Aqui construo o modelo analítico em **esquema estrela**, com duas tabelas fato e três dimensões:
# MAGIC
# MAGIC - `dim_item` — os 27 itens do SD3, com traço medido e indicação de pontuação invertida;
# MAGIC - `dim_respondente` — um registro por respondente, com as variáveis contextuais decodificadas;
# MAGIC - `dim_perfil` — as oito combinações possíveis de traços elevados;
# MAGIC - `fato_resposta_item` — uma linha por resposta a cada item, no grão mais fino do dado;
# MAGIC - `fato_escore_respondente` — os escores por traço e o perfil de cada respondente.
# MAGIC
# MAGIC É nesta camada que aplico as duas decisões analíticas que deixei de fora da Silver: a **inversão dos itens** e o **cálculo dos escores**.

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table("dark_triad.silver.sd3_respostas")
itens = [f"{letra}{i}" for letra in ["M", "N", "P"] for i in range(1, 10)]

print("Registros na Silver:", df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `dim_item` — o dicionário do instrumento
# MAGIC
# MAGIC Monto esta dimensão a partir do codebook que acompanha o dataset. Além do texto de cada item, registro duas informações essenciais para a análise: o traço que ele mede e se a pontuação é invertida.
# MAGIC
# MAGIC **Por que existem itens invertidos?** Instrumentos de autorrelato costumam incluir itens redigidos no sentido contrário ao construto, justamente para quebrar o piloto automático de quem responde tudo concordando. No SD3, cinco itens são assim: N2, N6 e N8 no narcisismo e P2 e P7 na psicopatia. Concordar com "eu odeio ser o centro das atenções" indica *menos* narcisismo, não mais.
# MAGIC
# MAGIC Sem essa correção, os escores desses traços ficariam distorcidos — por isso esta dimensão não é decorativa: ela é o que torna o cálculo correto.

# COMMAND ----------

itens_sd3 = [
    ("M1", "Maquiavelismo", "It's not wise to tell your secrets.", False),
    ("M2", "Maquiavelismo", "I like to use clever manipulation to get my way.", False),
    ("M3", "Maquiavelismo", "Whatever it takes, you must get the important people on your side.", False),
    ("M4", "Maquiavelismo", "Avoid direct conflict with others because they may be useful in the future.", False),
    ("M5", "Maquiavelismo", "It's wise to keep track of information that you can use against people later.", False),
    ("M6", "Maquiavelismo", "You should wait for the right time to get back at people.", False),
    ("M7", "Maquiavelismo", "There are things you should hide from other people because they don't need to know.", False),
    ("M8", "Maquiavelismo", "Make sure your plans benefit you, not others.", False),
    ("M9", "Maquiavelismo", "Most people can be manipulated.", False),
    ("N1", "Narcisismo", "People see me as a natural leader.", False),
    ("N2", "Narcisismo", "I hate being the center of attention.", True),
    ("N3", "Narcisismo", "Many group activities tend to be dull without me.", False),
    ("N4", "Narcisismo", "I know that I am special because everyone keeps telling me so.", False),
    ("N5", "Narcisismo", "I like to get acquainted with important people.", False),
    ("N6", "Narcisismo", "I feel embarrassed if someone compliments me.", True),
    ("N7", "Narcisismo", "I have been compared to famous people.", False),
    ("N8", "Narcisismo", "I am an average person.", True),
    ("N9", "Narcisismo", "I insist on getting the respect I deserve.", False),
    ("P1", "Psicopatia", "I like to get revenge on authorities.", False),
    ("P2", "Psicopatia", "I avoid dangerous situations.", True),
    ("P3", "Psicopatia", "Payback needs to be quick and nasty.", False),
    ("P4", "Psicopatia", "People often say I'm out of control.", False),
    ("P5", "Psicopatia", "It's true that I can be mean to others.", False),
    ("P6", "Psicopatia", "People who mess with me always regret it.", False),
    ("P7", "Psicopatia", "I have never gotten into trouble with the law.", True),
    ("P8", "Psicopatia", "I enjoy having sex with people I hardly know.", False),
    ("P9", "Psicopatia", "I'll say anything to get what I want.", False),
]

dim_item = spark.createDataFrame(itens_sd3, ["id_item", "traco", "texto_item", "invertido"])

(
    dim_item.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dark_triad.gold.dim_item")
)

display(dim_item)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `dim_respondente` — quem respondeu
# MAGIC
# MAGIC Decodifico aqui a variável `source` conforme o codebook: 1 significa que a pessoa chegou pela página inicial do site, 2 por busca no Google e 3 por outras origens, tudo inferido pelo referenciador HTTP.
# MAGIC
# MAGIC Sobre `country`, registro uma ressalva importante: ele vem de geolocalização por IP, ou seja, indica de onde a pessoa acessou, não a nacionalidade dela. Uso essa variável apenas como aproximação de contexto geográfico.

# COMMAND ----------

dim_respondente = df_silver.select(
    "id_respondente",
    F.col("country").alias("pais_acesso"),
    F.col("source").alias("id_origem"),
    F.when(F.col("source") == 1, "Página inicial do site")
     .when(F.col("source") == 2, "Busca no Google")
     .when(F.col("source") == 3, "Outra origem")
     .otherwise("Não identificada").alias("origem_acesso"),
    F.col("_data_ingestao").alias("data_ingestao"),
)

(
    dim_respondente.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dark_triad.gold.dim_respondente")
)

display(dim_respondente.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `fato_resposta_item` 
# MAGIC
# MAGIC Transformo as 27 colunas de itens em linhas (`stack`), de modo que cada linha seja uma resposta de uma pessoa a uma pergunta. Esse formato longo é o que permite cruzar com `dim_item` e analisar item a item.
# MAGIC
# MAGIC Guardo dois valores lado a lado: a `resposta_original`, como a pessoa marcou, e a `resposta_corrigida`, já com a inversão aplicada nos cinco itens invertidos pela fórmula `6 − resposta`. Manter as duas preserva a rastreabilidade: sempre dá para conferir a conta.

# COMMAND ----------

expr_stack = "stack({}, {}) as (id_item, resposta_original)".format(
    len(itens),
    ", ".join([f"'{c}', {c}" for c in itens])
)

fato_resposta_item = (
    df_silver.select("id_respondente", F.expr(expr_stack))
    .join(spark.table("dark_triad.gold.dim_item").select("id_item", "traco", "invertido"), on="id_item")
    .withColumn(
        "resposta_corrigida",
        F.when(F.col("invertido"), 6 - F.col("resposta_original")).otherwise(F.col("resposta_original"))
    )
    .select("id_respondente", "id_item", "traco", "resposta_original", "resposta_corrigida")
)

(
    fato_resposta_item.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dark_triad.gold.fato_resposta_item")
)

print("Linhas:", spark.table("dark_triad.gold.fato_resposta_item").count())
display(spark.table("dark_triad.gold.fato_resposta_item").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência da inversão
# MAGIC
# MAGIC Antes de seguir, confirmo que a inversão foi aplicada exatamente nos cinco itens previstos e em nenhum outro.

# COMMAND ----------

display(
    spark.table("dark_triad.gold.fato_resposta_item")
    .groupBy("id_item")
    .agg(F.sum(F.when(F.col("resposta_original") != F.col("resposta_corrigida"), 1).otherwise(0)).alias("respostas_alteradas"))
    .filter(F.col("respostas_alteradas") > 0)
    .orderBy("id_item")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escores por traço
# MAGIC
# MAGIC Calculo o escore de cada traço como a **média dos 9 itens corrigidos**. Uso a média, e não a soma, porque ela mantém o resultado na mesma escala dos itens (1 a 5), o que torna os três traços diretamente comparáveis entre si e a interpretação mais intuitiva.

# COMMAND ----------

escores = (
    spark.table("dark_triad.gold.fato_resposta_item")
    .groupBy("id_respondente")
    .pivot("traco", ["Maquiavelismo", "Narcisismo", "Psicopatia"])
    .agg(F.round(F.avg("resposta_corrigida"), 4))
    .withColumnRenamed("Maquiavelismo", "escore_maquiavelismo")
    .withColumnRenamed("Narcisismo", "escore_narcisismo")
    .withColumnRenamed("Psicopatia", "escore_psicopatia")
)

display(escores.summary("count", "mean", "stddev", "min", "50%", "max"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Definição de escore elevado
# MAGIC
# MAGIC O SD3 não possui ponto de corte clínico, então preciso definir um critério e assumi-lo explicitamente. Adoto o **percentil 75 da própria amostra** para cada traço: considero elevado quem está no quarto superior daquela distribuição.
# MAGIC
# MAGIC É um critério **relativo**, e faço questão de registrar isso: não significa que a pessoa tenha um nível clinicamente relevante do traço, apenas que pontuou mais que três quartos dos respondentes desta amostra. Um corte relativo também tem a vantagem de não depender de normas populacionais que não existem para este conjunto de dados.

# COMMAND ----------

cortes = {}
for traco, coluna in [
    ("Maquiavelismo", "escore_maquiavelismo"),
    ("Narcisismo", "escore_narcisismo"),
    ("Psicopatia", "escore_psicopatia"),
]:
    cortes[coluna] = escores.approxQuantile(coluna, [0.75], 0.001)[0]
    print(f"{traco}: P75 = {cortes[coluna]:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `dim_perfil` e `fato_escore_respondente`
# MAGIC
# MAGIC Com os três traços classificados como elevados ou não, cada respondente cai em uma das oito combinações possíveis. Codifico essa combinação em um `id_perfil` e crio a dimensão que a descreve.
# MAGIC
# MAGIC É essa estrutura que permite responder à pergunta sobre coocorrência: quantas pessoas concentram dois traços, quantas apresentam a tríade completa e quantas não pontuam alto em nenhum.

# COMMAND ----------

perfis = [
    (0, "Nenhum traço elevado", 0),
    (1, "Apenas psicopatia", 1),
    (2, "Apenas narcisismo", 1),
    (3, "Narcisismo + psicopatia", 2),
    (4, "Apenas maquiavelismo", 1),
    (5, "Maquiavelismo + psicopatia", 2),
    (6, "Maquiavelismo + narcisismo", 2),
    (7, "Tríade completa", 3),
]

dim_perfil = spark.createDataFrame(perfis, ["id_perfil", "descricao_perfil", "qtd_tracos_elevados"])

(
    dim_perfil.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dark_triad.gold.dim_perfil")
)

display(dim_perfil)

# COMMAND ----------

fato_escore = (
    escores
    .withColumn("elevado_maquiavelismo", F.col("escore_maquiavelismo") > F.lit(cortes["escore_maquiavelismo"]))
    .withColumn("elevado_narcisismo", F.col("escore_narcisismo") > F.lit(cortes["escore_narcisismo"]))
    .withColumn("elevado_psicopatia", F.col("escore_psicopatia") > F.lit(cortes["escore_psicopatia"]))
)

fato_escore = fato_escore.withColumn(
    "id_perfil",
    F.col("elevado_maquiavelismo").cast("int") * 4
    + F.col("elevado_narcisismo").cast("int") * 2
    + F.col("elevado_psicopatia").cast("int")
)

(
    fato_escore.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dark_triad.gold.fato_escore_respondente")
)

display(fato_escore.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Documentação no Unity Catalog
# MAGIC
# MAGIC Registro a descrição de cada tabela para que o catálogo explique o modelo sem depender deste notebook.

# COMMAND ----------

descricoes = {
    "dim_item": "Dimensão dos 27 itens do SD3, com o traço medido, o enunciado original e a indicação de pontuação invertida (N2, N6, N8, P2, P7). Construída a partir do codebook do dataset.",
    "dim_respondente": "Dimensão de respondentes válidos, com país inferido por geolocalização de IP (MaxMind GeoLite) e origem do acesso ao teste decodificada conforme o codebook.",
    "dim_perfil": "Dimensão das oito combinações possíveis de traços elevados, do perfil sem nenhum traço elevado à tríade completa.",
    "fato_resposta_item": "Fato no grão de resposta por item: uma linha por respondente e item, com a resposta original e a resposta corrigida após inversão dos itens reversos.",
    "fato_escore_respondente": "Fato no grão de respondente: escores médios (1 a 5) por traço, indicadores de escore elevado segundo o percentil 75 da amostra e o perfil de combinação resultante.",
}

for tabela, descricao in descricoes.items():
    spark.sql(f"COMMENT ON TABLE dark_triad.gold.{tabela} IS '{descricao}'")

display(spark.sql("SHOW TABLES IN dark_triad.gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado desta etapa
# MAGIC
# MAGIC - Modelo em esquema estrela persistido em `dark_triad.gold`, com três dimensões e dois fatos.
# MAGIC - Itens invertidos corrigidos e escores calculados na escala original do instrumento.
# MAGIC - Critério de escore elevado definido e documentado.
# MAGIC
# MAGIC **Próximo notebook:** `06_analise` — consultas que respondem às perguntas de negócio.