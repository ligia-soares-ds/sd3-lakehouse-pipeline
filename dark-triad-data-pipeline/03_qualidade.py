# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 — Qualidade de Dados
# MAGIC
# MAGIC **Etapa 4.5 do MVP — Qualidade**
# MAGIC
# MAGIC Este notebook **diagnostica** a camada Bronze, sem alterá-la. Analisar antes de limpar é o que permite justificar cada decisão de tratamento na camada Silver: primeiro se descreve o problema, depois se escolhe a conduta.
# MAGIC
# MAGIC As cinco dimensões avaliadas são: **completude**, **consistência**, **unicidade**, **acurácia** e **outliers**.
# MAGIC
# MAGIC Um cuidado específico deste tipo de dado: escalas de traços sombrios são sensíveis ao **viés de desejabilidade social**, e há registro de respondentes que assinalam a opção mais baixa em todos os itens mesmo sob anonimato. Por isso, padrões de resposta uniformes recebem atenção especial na seção de acurácia.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preparação
# MAGIC
# MAGIC A Bronze guarda tudo como texto. Para as verificações numéricas, é criada uma **visão temporária** com as colunas convertidas para inteiro. A conversão vive apenas nesta sessão, sendo que a tabela original permanece intacta.
# MAGIC
# MAGIC Um detalhe importante: `cast("int")` devolve `null` quando o valor não é numérico. Isso é útil aqui, porque separa o que é ausência real do que é conteúdo inesperado.

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("dark_triad.bronze.sd3_raw")

itens = [f"{letra}{i}" for letra in ["M", "N", "P"] for i in range(1, 10)]

df_num = df.select(
    *[F.col(c).cast("int").alias(c) for c in itens],
    F.col("country"),
    F.col("source").cast("int").alias("source"),
)
df_num.createOrReplaceTempView("sd3_num")

print(f"Registros: {df_num.count()} | Itens analisados: {len(itens)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude
# MAGIC
# MAGIC Duas formas de ausência convivem neste dataset:
# MAGIC
# MAGIC - **nulos ou vazios**: o respondente não forneceu o dado;
# MAGIC - **zeros**: a escala do SD3 vai de 1 a 5, então `0` costuma indicar item **não respondido**, e não uma resposta válida.
# MAGIC
# MAGIC Tratar o zero como se fosse resposta rebaixaria artificialmente os escores, então ele é contabilizado separadamente.

# COMMAND ----------

completude = df_num.select([
    F.struct(
        F.lit(c).alias("campo"),
        F.sum(F.col(c).isNull().cast("int")).alias("nulos"),
        F.sum((F.col(c) == 0).cast("int")).alias("zeros"),
    ).alias(c)
    for c in itens
])

linha = completude.collect()[0]
total = df_num.count()

resultado_completude = spark.createDataFrame([
    (r["campo"], r["nulos"], r["zeros"], round(100 * (r["nulos"] + r["zeros"]) / total, 2))
    for r in linha
], ["campo", "nulos", "zeros", "pct_ausente"])

display(resultado_completude.orderBy(F.desc("pct_ausente")))

# COMMAND ----------

df_zeros = df_num.withColumn(
    "qtd_zeros",
    sum([(F.col(c) == 0).cast("int") for c in itens])
)

display(df_zeros.groupBy("qtd_zeros").count().orderBy("qtd_zeros"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Completude das colunas contextuais

# COMMAND ----------

display(df.select(
    F.sum((F.col("country").isNull() | (F.trim(F.col("country")) == "")).cast("int")).alias("country_ausente"),
    F.sum(F.col("source").isNull().cast("int")).alias("source_ausente"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consistência
# MAGIC
# MAGIC Todo item do SD3 deve conter um valor inteiro entre 1 e 5. Aqui são contados os registros com pelo menos um valor fora desse domínio (desconsiderando os zeros, já contabilizados como ausência).

# COMMAND ----------

condicao_fora = None
for c in itens:
    cond = (F.col(c) > 5) | (F.col(c) < 0)
    condicao_fora = cond if condicao_fora is None else (condicao_fora | cond)

fora_escala = df_num.filter(condicao_fora)
print("Registros com algum valor fora do domínio 1–5:", fora_escala.count())
display(fora_escala.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Domínio observado por item
# MAGIC
# MAGIC Mínimo, máximo e média de cada item. Além de verificar a consistência, esta tabela alimenta a coluna "domínio de valores" do **Catálogo de Dados**.

# COMMAND ----------

dominio = df_num.select([
    F.struct(
        F.lit(c).alias("campo"),
        F.min(c).alias("minimo"),
        F.max(c).alias("maximo"),
        F.round(F.avg(F.when(F.col(c) > 0, F.col(c))), 2).alias("media_validos"),
    ).alias(c)
    for c in itens
]).collect()[0]

display(spark.createDataFrame(
    [(r["campo"], r["minimo"], r["maximo"], r["media_validos"]) for r in dominio],
    ["campo", "minimo", "maximo", "media_validos"]
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unicidade
# MAGIC
# MAGIC O dataset não possui identificador de respondente, então a duplicidade é avaliada pela repetição exata do conjunto de respostas.
# MAGIC
# MAGIC **Atenção à interpretação:** com 27 itens de 5 pontos, respostas idênticas por coincidência são improváveis — mas não impossíveis, especialmente em padrões extremos (como todos os itens iguais). Por isso, duplicatas são reportadas, e a decisão sobre removê-las é discutida na Silver.

# COMMAND ----------

total_linhas = df_num.count()
distintas = df_num.select(*itens).distinct().count()

print(f"Total de registros: {total_linhas}")
print(f"Combinações distintas de respostas: {distintas}")
print(f"Registros com resposta repetida: {total_linhas - distintas}")

display(
    df_num.groupBy(*itens).count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Acurácia — padrões de resposta suspeitos
# MAGIC
# MAGIC Aqui se verifica se as respostas fazem sentido como comportamento de preenchimento:
# MAGIC
# MAGIC - **Resposta uniforme (*straight-lining*)**: mesmo valor nos 27 itens. Pode indicar desengajamento ou, no caso específico destas escalas, negação defensiva dos traços (tudo no valor mínimo).
# MAGIC - **Baixa variabilidade**: desvio-padrão muito pequeno entre os itens, sugerindo pouca discriminação entre as perguntas.
# MAGIC
# MAGIC Estes casos são **sinalizados**, não descartados automaticamente: eles também são um dado sobre o instrumento.

# COMMAND ----------

colunas_itens = [F.col(c) for c in itens]

df_padroes = df_num.withColumn("valores_distintos", F.size(F.array_distinct(F.array(*colunas_itens))))

uniformes = df_padroes.filter(F.col("valores_distintos") == 1)
print("Respondentes com o mesmo valor nos 27 itens:", uniformes.count())

display(
    uniformes.groupBy(F.col("M1").alias("valor_repetido")).count().orderBy("valor_repetido")
)

# COMMAND ----------

display(
    df_padroes.groupBy("valores_distintos").count().orderBy("valores_distintos")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outliers
# MAGIC
# MAGIC Nos itens individuais não faz sentido falar em outlier, já que a escala é limitada a 1–5. A análise é feita sobre os **escores por traço** (média dos 9 itens), usando o critério clássico do intervalo interquartil (1,5 × IIQ).
# MAGIC
# MAGIC Os escores calculados aqui são **provisórios**, sem a inversão de itens — servem apenas para o diagnóstico. O cálculo definitivo acontece na camada Gold.

# COMMAND ----------

def media_traco(prefixo):
    cols = [F.when(F.col(f"{prefixo}{i}") > 0, F.col(f"{prefixo}{i}")) for i in range(1, 10)]
    soma = sum([F.coalesce(c, F.lit(0)) for c in cols])
    qtd = sum([F.when(c.isNotNull(), 1).otherwise(0) for c in cols])
    return F.when(qtd > 0, soma / qtd)

df_escores = df_num.select(
    media_traco("M").alias("maquiavelismo"),
    media_traco("N").alias("narcisismo"),
    media_traco("P").alias("psicopatia"),
)

display(df_escores.summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max"))

# COMMAND ----------

for traco in ["maquiavelismo", "narcisismo", "psicopatia"]:
    q1, q3 = df_escores.approxQuantile(traco, [0.25, 0.75], 0.001)
    iiq = q3 - q1
    limite_inf, limite_sup = q1 - 1.5 * iiq, q3 + 1.5 * iiq
    n_out = df_escores.filter((F.col(traco) < limite_inf) | (F.col(traco) > limite_sup)).count()
    print(f"{traco}: Q1={q1:.2f} Q3={q3:.2f} | limites=[{limite_inf:.2f}, {limite_sup:.2f}] | outliers={n_out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Panorama das variáveis contextuais
# MAGIC
# MAGIC Distribuição de `country` e `source`, que definem se a pergunta sobre diferenças entre grupos é viável e com qual granularidade.

# COMMAND ----------

display(
    df.groupBy("country").count().orderBy(F.desc("count")).limit(20)
)

# COMMAND ----------

display(
    df.groupBy("source").count().orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Síntese dos achados
# MAGIC
# MAGIC | Dimensão | Achado | Tratamento na Silver |
# MAGIC |---|---|---|
# MAGIC | Completude | 452 respondentes (2,5%) com ao menos um item em branco | Excluir esses registros |
# MAGIC | Consistência | Nenhum valor fora do domínio esperado | Nenhum tratamento |
# MAGIC | Unicidade | 59 registros com respostas idênticas a outras | Manter |
# MAGIC | Acurácia | 31 respondentes com o mesmo valor nos 27 itens | Excluir esses registros |
# MAGIC | Outliers | 136 (maquiavelismo), 291 (narcisismo) e 38 (psicopatia) escores abaixo do limite inferior | Manter |
# MAGIC
# MAGIC ### Detalhamento das decisões
# MAGIC
# MAGIC **Completude.** Não encontrei valores nulos, mas identifiquei o valor 0 em 452 respondentes. Como a escala do SD3 varia de 1 a 5, interpreto o 0 como item não respondido. A maioria deixou apenas um item em branco, mas 10 registros têm 17 ou mais itens faltantes, incluindo 6 questionários integralmente vazios. Optei por excluir todos os respondentes com qualquer item em branco: a perda é pequena e garante escores calculados sobre os 9 itens de cada traço, sem recorrer a imputação.
# MAGIC
# MAGIC **Consistência.** Todos os itens variam entre 0 e 5, sem nenhum valor fora do esperado, o que indica que o formulário de origem restringia as opções de resposta. A exclusão dos zeros já resolve o único valor fora da escala.
# MAGIC
# MAGIC **Unicidade.** Como o dataset não possui identificador de respondente, avaliei duplicidade pela repetição exata do conjunto de respostas: 18.133 combinações distintas em 18.192 registros. Ao inspecionar os 59 repetidos, verifiquei que são padrões extremos (todos os itens 5, 1, 3 ou 0), ou seja, coincidência entre respostas uniformes, e não duplicação técnica. Removê-los descartaria respostas legítimas, então optei por mantê-los e tratar a questão na dimensão seguinte.
# MAGIC
# MAGIC **Acurácia.** Encontrei 31 respondentes que marcaram o mesmo valor nos 27 itens: 12 responderam tudo 5, 6 tudo 1, 6 tudo 0, 5 tudo 3 e 2 tudo 4. Padrões uniformes indicam baixo engajamento com o conteúdo dos itens, comprometendo a validade individual da resposta, e por isso excluo esses registros. Vale registrar um contraponto interessante: a literatura enfatiza o viés de desejabilidade social, que levaria à negação dos traços, mas aqui o padrão de resposta máxima é o dobro do de resposta mínima.
# MAGIC
# MAGIC **Outliers.** Sobre os escores provisórios por traço, os casos extremos estão todos abaixo do limite inferior; não há outliers superiores, já que os limites calculados ultrapassam o máximo possível da escala. São escores baixos e plenamente possíveis dentro de 1 a 5, não erros de medida, então mantenho todos: descartá-los enviesaria a distribuição justamente na cauda que interessa descrever.
# MAGIC
# MAGIC ### Variáveis contextuais
# MAGIC
# MAGIC A amostra concentra-se em países de língua inglesa: Estados Unidos (9.252), Reino Unido (2.810), Canadá (1.213) e Austrália (760); o Brasil aparece com 109 respondentes. A variável `source` assume três valores (1 com 9.872, 3 com 5.591 e 2 com 2.729), cujo significado consta no codebook. Essa concentração limita comparações entre países e é retomada nas limitações da análise.
# MAGIC
# MAGIC **Próximo notebook:** `04_silver` — aplicação dos tratamentos decididos aqui e conversão para o formato de análise.
# MAGIC