# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 — Análise
# MAGIC
# MAGIC **Etapa 4.5 do MVP — respostas às perguntas de negócio**
# MAGIC
# MAGIC Com o modelo pronto na camada Gold, retomo aqui as perguntas que defini no objetivo e respondo uma a uma, discutindo o que cada resultado significa.
# MAGIC
# MAGIC Duas premissas que valem para toda a análise: os escores variam de 1 a 5 e já estão corrigidos quanto aos itens invertidos; e "escore elevado" significa estar acima do percentil 75 **desta amostra**, não de uma norma clínica.

# COMMAND ----------

from pyspark.sql import functions as F

escores = spark.table("dark_triad.gold.fato_escore_respondente")
respostas = spark.table("dark_triad.gold.fato_resposta_item")
itens = spark.table("dark_triad.gold.dim_item")
perfis = spark.table("dark_triad.gold.dim_perfil")
respondentes = spark.table("dark_triad.gold.dim_respondente")

total = escores.count()
print("Respondentes analisados:", total)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Qual traço apresenta os escores mais altos e como se distribuem?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Maquiavelismo' AS traco,
# MAGIC   ROUND(AVG(escore_maquiavelismo), 2) AS media,
# MAGIC   ROUND(STDDEV(escore_maquiavelismo), 2) AS desvio_padrao,
# MAGIC   ROUND(PERCENTILE(escore_maquiavelismo, 0.5), 2) AS mediana,
# MAGIC   ROUND(MIN(escore_maquiavelismo), 2) AS minimo,
# MAGIC   ROUND(MAX(escore_maquiavelismo), 2) AS maximo
# MAGIC FROM dark_triad.gold.fato_escore_respondente
# MAGIC UNION ALL
# MAGIC SELECT 'Narcisismo', ROUND(AVG(escore_narcisismo), 2), ROUND(STDDEV(escore_narcisismo), 2),
# MAGIC        ROUND(PERCENTILE(escore_narcisismo, 0.5), 2), ROUND(MIN(escore_narcisismo), 2), ROUND(MAX(escore_narcisismo), 2)
# MAGIC FROM dark_triad.gold.fato_escore_respondente
# MAGIC UNION ALL
# MAGIC SELECT 'Psicopatia', ROUND(AVG(escore_psicopatia), 2), ROUND(STDDEV(escore_psicopatia), 2),
# MAGIC        ROUND(PERCENTILE(escore_psicopatia, 0.5), 2), ROUND(MIN(escore_psicopatia), 2), ROUND(MAX(escore_psicopatia), 2)
# MAGIC FROM dark_triad.gold.fato_escore_respondente
# MAGIC ORDER BY media DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribuição por faixas de escore
# MAGIC
# MAGIC A média sozinha esconde o formato da distribuição, então agrupo os escores em faixas de meio ponto para enxergar onde as pessoas se concentram em cada traço.

# COMMAND ----------

faixas = None
for traco, coluna in [
    ("Maquiavelismo", "escore_maquiavelismo"),
    ("Narcisismo", "escore_narcisismo"),
    ("Psicopatia", "escore_psicopatia"),
]:
    parcial = (
        escores
        .withColumn("faixa", (F.floor(F.col(coluna) * 2) / 2))
        .groupBy("faixa")
        .agg(F.count("*").alias("qtd"))
        .withColumn("traco", F.lit(traco))
        .withColumn("percentual", F.round(100 * F.col("qtd") / total, 2))
    )
    faixas = parcial if faixas is None else faixas.unionByName(parcial)

display(faixas.orderBy("traco", "faixa"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Discussão
# MAGIC
# MAGIC O maquiavelismo foi o traço mais intenso desta amostra (média 3,71; mediana 3,78), seguido do narcisismo (3,07) e da psicopatia (2,81). A distância entre o primeiro e o último é de quase um ponto inteiro em uma escala de cinco, o que é considerável.
# MAGIC
# MAGIC Uma leitura possível é que os três traços não são igualmente fáceis de admitir. Os itens de maquiavelismo descrevem estratégia interpessoal — guardar informações, escolher a hora certa, cultivar pessoas influentes — e podem ser lidos como prudência ou inteligência social, algo que não ameaça a autoimagem moral de quem responde. Já os itens de psicopatia envolvem admitir crueldade, descontrole e prazer em vingança, o que é socialmente muito mais custoso. Essa assimetria é coerente com a literatura que discute o viés de desejabilidade social nas escalas de traços sombrios (Coutinho, [2026]), segundo a qual esses instrumentos são facilmente falseáveis de forma intencional.
# MAGIC
# MAGIC As distribuições também diferem em formato: o maquiavelismo concentra-se em faixas altas, enquanto a psicopatia se concentra em faixas baixas, com cauda à direita. Ou seja, escores elevados de psicopatia existem, mas são minoria clara nesta amostra.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Que proporção apresenta escore elevado em cada traço?
# MAGIC
# MAGIC Por construção, o corte no percentil 75 coloca cerca de 25% da amostra em cada traço. O que interessa aqui não é o percentual em si, mas o **valor de escore** que cada corte exige: ele mostra o quanto a régua muda de traço para traço.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Maquiavelismo' AS traco,
# MAGIC   SUM(CAST(elevado_maquiavelismo AS INT)) AS qtd_elevados,
# MAGIC   ROUND(100 * AVG(CAST(elevado_maquiavelismo AS INT)), 2) AS pct_elevados
# MAGIC FROM dark_triad.gold.fato_escore_respondente
# MAGIC UNION ALL
# MAGIC SELECT 'Narcisismo', SUM(CAST(elevado_narcisismo AS INT)), ROUND(100 * AVG(CAST(elevado_narcisismo AS INT)), 2)
# MAGIC FROM dark_triad.gold.fato_escore_respondente
# MAGIC UNION ALL
# MAGIC SELECT 'Psicopatia', SUM(CAST(elevado_psicopatia AS INT)), ROUND(100 * AVG(CAST(elevado_psicopatia AS INT)), 2)
# MAGIC FROM dark_triad.gold.fato_escore_respondente

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Discussão
# MAGIC
# MAGIC Como o critério adotado foi o percentil 75, cerca de um quarto da amostra é classificado como elevado em cada traço, por construção. O achado relevante não está no percentual, e sim no **valor de escore exigido por cada corte**: 4,33 para maquiavelismo, 3,56 para narcisismo e 3,33 para psicopatia.
# MAGIC
# MAGIC Isso mostra que "estar entre os 25% mais altos" significa coisas bem diferentes dependendo do traço. Para entrar no quarto superior de maquiavelismo é preciso concordar fortemente com quase todos os itens; já no caso da psicopatia, basta uma pontuação próxima do ponto neutro da escala. Um corte absoluto idêntico para os três traços — por exemplo, escore acima de 4 — classificaria quase ninguém como elevado em psicopatia e distorceria a comparação.
# MAGIC
# MAGIC Esse é justamente o motivo de eu assumir o critério como relativo e descritivo. O SD3 não dispõe de pontos de corte clínicos, e tratar esses limites como se fossem diagnósticos seria exatamente o tipo de leitura patologizante que a literatura da área recomenda evitar: os traços sombrios são dimensionais e estão presentes em todas as pessoas em algum grau (Coutinho, [2026]).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Quais pares de traços se associam mais?
# MAGIC
# MAGIC Esta é a pergunta central do trabalho. Calculo a correlação de Pearson entre os escores, par a par.
# MAGIC
# MAGIC Como referência de leitura: correlações em torno de 0,10 são fracas, 0,30 moderadas e 0,50 fortes, seguindo convenções usuais em psicologia.

# COMMAND ----------

correlacoes = [
    ("Maquiavelismo × Psicopatia", escores.stat.corr("escore_maquiavelismo", "escore_psicopatia")),
    ("Maquiavelismo × Narcisismo", escores.stat.corr("escore_maquiavelismo", "escore_narcisismo")),
    ("Narcisismo × Psicopatia", escores.stat.corr("escore_narcisismo", "escore_psicopatia")),
]

display(
    spark.createDataFrame([(par, round(valor, 4)) for par, valor in correlacoes], ["par_de_tracos", "correlacao"])
    .orderBy(F.desc("correlacao"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coocorrência de escores elevados
# MAGIC
# MAGIC A correlação descreve a relação ao longo de toda a escala. Complemento com uma leitura mais concreta: entre quem tem um traço elevado, qual a chance de ter também o outro?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Elevado em maquiavelismo → também em psicopatia' AS relacao,
# MAGIC   ROUND(100 * AVG(CAST(elevado_psicopatia AS INT)), 2) AS pct
# MAGIC FROM dark_triad.gold.fato_escore_respondente WHERE elevado_maquiavelismo
# MAGIC UNION ALL
# MAGIC SELECT 'Elevado em maquiavelismo → também em narcisismo', ROUND(100 * AVG(CAST(elevado_narcisismo AS INT)), 2)
# MAGIC FROM dark_triad.gold.fato_escore_respondente WHERE elevado_maquiavelismo
# MAGIC UNION ALL
# MAGIC SELECT 'Elevado em narcisismo → também em psicopatia', ROUND(100 * AVG(CAST(elevado_psicopatia AS INT)), 2)
# MAGIC FROM dark_triad.gold.fato_escore_respondente WHERE elevado_narcisismo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Discussão
# MAGIC
# MAGIC Este é o achado central do trabalho. As três correlações são positivas e de magnitude expressiva, mas a hierarquia entre elas é o que interessa:
# MAGIC
# MAGIC - Maquiavelismo × Psicopatia: **0,67**
# MAGIC - Narcisismo × Psicopatia: 0,57
# MAGIC - Maquiavelismo × Narcisismo: 0,51
# MAGIC
# MAGIC O par maquiavelismo–psicopatia é o mais associado, com folga, e confirma a hipótese que registrei antes da análise. O resultado converge com metanálises discutidas por Coutinho ([2026]): a de Vize e colaboradores conclui que o maquiavelismo não seria um traço verdadeiramente independente, sendo mais bem compreendido como uma variação da psicopatia, e a de Knitter e colaboradores identifica sobreposição conceitual semelhante. O narcisismo, por sua vez, aparece como o traço mais distinto dos três — também aqui, já que suas duas correlações são as mais baixas.
# MAGIC
# MAGIC Uma correlação de 0,67 entre construtos que se pretendem distintos é alta o suficiente para sustentar a dúvida levantada por esses autores sobre a real independência dos três fatores. Ao mesmo tempo, o fato de as três correlações serem substanciais dá apoio empírico à noção de **fator D**, o núcleo comum descrito na literatura como a propensão a maximizar benefícios pessoais às custas de terceiros.
# MAGIC
# MAGIC A leitura em termos de coocorrência torna isso mais concreto: entre os respondentes com maquiavelismo elevado, 57,6% também apresentam psicopatia elevada e 53,1% apresentam narcisismo elevado. Como o acaso produziria cerca de 25%, todos os pares ocorrem muito acima do esperado.
# MAGIC
# MAGIC Vale registrar uma ressalva de método: correlação não é evidência de que os construtos sejam o mesmo. Parte da associação pode vir de um fator de estilo de resposta — quem tende a concordar (ou a negar) responde de forma parecida em todos os itens, inflando as correlações entre as escalas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Como se distribuem os perfis de combinação?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.descricao_perfil,
# MAGIC   p.qtd_tracos_elevados,
# MAGIC   COUNT(*) AS qtd_respondentes,
# MAGIC   ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentual
# MAGIC FROM dark_triad.gold.fato_escore_respondente f
# MAGIC JOIN dark_triad.gold.dim_perfil p ON f.id_perfil = p.id_perfil
# MAGIC GROUP BY p.descricao_perfil, p.qtd_tracos_elevados
# MAGIC ORDER BY qtd_respondentes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quantos traços elevados por pessoa

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.qtd_tracos_elevados,
# MAGIC   COUNT(*) AS qtd_respondentes,
# MAGIC   ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentual
# MAGIC FROM dark_triad.gold.fato_escore_respondente f
# MAGIC JOIN dark_triad.gold.dim_perfil p ON f.id_perfil = p.id_perfil
# MAGIC GROUP BY p.qtd_tracos_elevados
# MAGIC ORDER BY p.qtd_tracos_elevados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Discussão
# MAGIC
# MAGIC A distribuição dos perfis traz o resultado mais chamativo da análise. A maioria (58%) não apresenta nenhum traço elevado, o que já era esperado pelo próprio critério de corte. Entre os demais, porém, a **tríade completa aparece como o terceiro perfil mais frequente** (8,19%), atrás apenas de "apenas narcisismo" (8,38%) e à frente de qualquer combinação de exatamente dois traços.
# MAGIC
# MAGIC Se os três traços fossem independentes entre si, a tríade completa seria o perfil mais raro, e não um dos mais comuns. O padrão observado indica o contrário: quando alguém pontua alto, tende a pontuar alto em tudo. Isso é consistente com a sobreposição discutida na literatura e com a hipótese de um núcleo comum aos três traços.
# MAGIC
# MAGIC Entre as combinações de dois traços, a mais frequente é narcisismo + psicopatia (4,99%), seguida de maquiavelismo + psicopatia (4,26%) e maquiavelismo + narcisismo (3,28%): novamente com a psicopatia participando dos dois pares mais comuns.
# MAGIC
# MAGIC Uma observação relevante para a interpretação: esses perfis descrevem posições relativas dentro desta amostra, não categorias clínicas. Chamar alguém de "tríade completa" aqui significa apenas que a pessoa está no quarto superior dos três traços entre quem respondeu ao teste online.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Quais itens são mais endossados dentro de cada traço?
# MAGIC
# MAGIC Uso a resposta corrigida, para que valores altos signifiquem sempre mais do traço. Itens com médias muito altas ou muito baixas dizem algo sobre o que as pessoas admitem com facilidade e o que tendem a rejeitar.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   r.traco,
# MAGIC   r.id_item,
# MAGIC   i.texto_item,
# MAGIC   i.invertido,
# MAGIC   ROUND(AVG(r.resposta_corrigida), 2) AS media_corrigida,
# MAGIC   ROUND(100 * AVG(CASE WHEN r.resposta_corrigida >= 4 THEN 1 ELSE 0 END), 2) AS pct_concordancia
# MAGIC FROM dark_triad.gold.fato_resposta_item r
# MAGIC JOIN dark_triad.gold.dim_item i ON r.id_item = i.id_item
# MAGIC GROUP BY r.traco, r.id_item, i.texto_item, i.invertido
# MAGIC ORDER BY r.traco, media_corrigida DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** ## Pergunta 5 — Discussão
# MAGIC
# MAGIC Os itens mais endossados revelam um padrão consistente com o que a análise anterior já sugeria: o que as pessoas admitem com facilidade não é a conduta em si, mas a **leitura de mundo** que a justifica.
# MAGIC
# MAGIC No maquiavelismo, os três primeiros colocados são justamente os mais impessoais: guardar segredos (M7, média 4,39, com 89% de concordância), não contar segredos (M1, 4,18) e acreditar que a maioria das pessoas pode ser manipulada (M9, 4,08). Nenhum deles exige assumir uma conduta manipuladora própria — são princípios gerais de prudência ou diagnósticos sobre os outros. Já o item que fecha a lista, "garanta que seus planos beneficiem você, não os outros" (M8, 3,17), é o que mais explicita o autointeresse em detrimento alheio. O mesmo vale para "gosto de usar manipulação inteligente para conseguir o que quero" (M2, 3,49), que apesar de nomear o comportamento, ainda o enquadra como habilidade estratégica. Esse perfil dialoga com a descrição do maquiavélico como estrategista: alguém que planeja, antecipa e calcula, sem recorrer ao confronto direto.
# MAGIC
# MAGIC Na psicopatia, o item mais endossado surpreende: "é verdade que posso ser mau com os outros" (P5, 3,72, com 68% de concordância) fica isolado no topo, muito acima do segundo colocado. Admitir a capacidade de ser cruel, portanto, não é tabu — o que as pessoas rejeitam é a perda de controle e a transgressão: "as pessoas costumam dizer que estou fora de controle" (P4, 2,32) é o item menos endossado de todo o instrumento, e "a retaliação precisa ser rápida e cruel" (P3, 2,62) também fica na base. A diferença entre poder ser mau e ser descontrolado sugere que o que se preserva na autoimagem é a **agência**, não a bondade.
# MAGIC
# MAGIC No narcisismo, os itens mais endossados são os socialmente legítimos: gostar de conhecer pessoas importantes (N5, 3,51) e fazer questão do respeito merecido (N9, 3,45). Os que afirmam superioridade explícita ficam no fim: saber-se especial porque todos dizem isso (N4, 2,68) e achar que atividades em grupo são sem graça sem a própria presença (N3, 2,65). Ou seja, ambição e autoestima passam; grandiosidade declarada, não.
# MAGIC
# MAGIC Os itens invertidos merecem nota à parte, porque se comportam de modo coerente: após a correção, N6 (2,90), N2 (2,88) e P2 (2,79) ficam todos abaixo da média de seus traços. Como concordar com eles significa menos do traço, esse resultado indica que a maioria dos respondentes de fato discordou pouco dessas afirmações socialmente simpáticas — o que é exatamente o efeito que itens reversos buscam captar.
# MAGIC
# MAGIC Em conjunto, o contraste entre topo e base das listas é a melhor ilustração empírica, dentro deste trabalho, do viés de desejabilidade social discutido na literatura: o gradiente de endosso acompanha menos a intensidade do traço e mais o custo social de admiti-lo.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — Há diferenças entre grupos de respondentes?
# MAGIC
# MAGIC Esta é a pergunta que exige mais cautela. O país vem de geolocalização por IP, a amostra é autosselecionada e fortemente concentrada em países de língua inglesa. Por isso limito a comparação aos países com volume razoável e trato o resultado como descritivo, sem qualquer leitura sobre populações nacionais.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.pais_acesso,
# MAGIC   COUNT(*) AS respondentes,
# MAGIC   ROUND(AVG(f.escore_maquiavelismo), 2) AS maquiavelismo,
# MAGIC   ROUND(AVG(f.escore_narcisismo), 2) AS narcisismo,
# MAGIC   ROUND(AVG(f.escore_psicopatia), 2) AS psicopatia
# MAGIC FROM dark_triad.gold.fato_escore_respondente f
# MAGIC JOIN dark_triad.gold.dim_respondente d ON f.id_respondente = d.id_respondente
# MAGIC GROUP BY d.pais_acesso
# MAGIC HAVING COUNT(*) >= 200
# MAGIC ORDER BY respondentes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por origem do acesso ao teste
# MAGIC
# MAGIC Aqui a comparação é menos arriscada, porque não envolve nenhum grupo social: apenas como a pessoa chegou ao questionário.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.origem_acesso,
# MAGIC   COUNT(*) AS respondentes,
# MAGIC   ROUND(AVG(f.escore_maquiavelismo), 2) AS maquiavelismo,
# MAGIC   ROUND(AVG(f.escore_narcisismo), 2) AS narcisismo,
# MAGIC   ROUND(AVG(f.escore_psicopatia), 2) AS psicopatia
# MAGIC FROM dark_triad.gold.fato_escore_respondente f
# MAGIC JOIN dark_triad.gold.dim_respondente d ON f.id_respondente = d.id_respondente
# MAGIC GROUP BY d.origem_acesso
# MAGIC ORDER BY respondentes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — Discussão
# MAGIC
# MAGIC As médias por país de acesso variam pouco: o maquiavelismo fica entre 3,42 (Finlândia) e 3,92 (Filipinas), o narcisismo entre 2,66 e 3,22 e a psicopatia entre 2,56 e 2,88. Os quatro países com maior volume — Estados Unidos (8.996), Reino Unido (2.724), Canadá (1.182) e Austrália (746) — praticamente não se distinguem entre si.
# MAGIC
# MAGIC Essa variação não sustenta nenhuma conclusão sobre populações nacionais, por três razões que se somam: a amostra é autosselecionada, e quem procura um teste de personalidade online não representa a população de nenhum país; o campo indica o local de acesso inferido por IP, não a nacionalidade do respondente; e os volumes são muito desiguais, com países de língua inglesa concentrando a maior parte dos casos, enquanto os demais aparecem com poucas centenas. Tratar diferenças de uma ou duas décimas como característica nacional seria transformar ruído amostral em estereótipo — exatamente o tipo de leitura que a literatura da área recomenda evitar, inclusive porque há problemas conhecidos de replicação de modelos de personalidade entre culturas.
# MAGIC
# MAGIC A comparação por origem do acesso é metodologicamente mais segura, já que não envolve nenhum grupo social. Quem chegou por busca no Google (2.639 respondentes) apresenta as médias mais altas nos três traços (maquiavelismo 3,78, narcisismo 3,22, psicopatia 2,92), acima de quem chegou pela página inicial do site (9.646 respondentes; 3,72, 3,03 e 2,80) ou por outra origem (5.430; 3,67, 3,07 e 2,79). As diferenças são pequenas, mas o padrão é consistente nos três traços.
# MAGIC
# MAGIC Uma hipótese plausível é de autosseleção por motivação: quem busca ativamente por um teste de tríade sombria pode já suspeitar de algo sobre si ou ter interesse específico no tema, enquanto quem apenas encontrou o questionário navegando pelo site responde por curiosidade genérica. É uma interpretação, não uma conclusão — o dado disponível registra a rota de chegada, não a intenção de quem respondeu.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC
# MAGIC O objetivo deste trabalho era compreender como os traços da tríade sombria se distribuem e se combinam em uma amostra ampla de respondentes do SD3. Reunindo as respostas, chego a duas conclusões que se complementam: **os três traços se distinguem com clareza em intensidade, mas são pouco independentes entre si**.
# MAGIC
# MAGIC Em intensidade, a ordem é nítida. O maquiavelismo lidera com média 3,71, seguido do narcisismo (3,07) e da psicopatia (2,81) — quase um ponto inteiro separando o primeiro do último em uma escala de cinco. A análise item a item mostrou que essa hierarquia acompanha menos a intensidade do traço e mais o custo social de admiti-lo: os itens mais endossados são os que descrevem princípios gerais ou leituras sobre os outros, enquanto os menos endossados exigem admitir descontrole, crueldade deliberada ou superioridade explícita. É um resultado que vale registrar com cuidado, porque ele diz respeito tanto ao que se mede quanto ao instrumento que mede.
# MAGIC
# MAGIC Em combinação, os traços caminham juntos com frequência muito acima do acaso. As três correlações são substanciais, o par maquiavelismo–psicopatia se destaca com 0,67 e a tríade completa aparece como o terceiro perfil mais comum (8,19%), à frente de qualquer combinação de exatamente dois traços. Entre quem tem maquiavelismo elevado, 57,6% também têm psicopatia elevada, contra os 25% que o acaso produziria. Se os construtos fossem realmente independentes, nada disso apareceria com essa força.
# MAGIC
# MAGIC Esse conjunto dialoga diretamente com o debate atual da área, que questiona se há mesmo três construtos distintos sob o guarda-chuva da tríade sombria — com metanálises sugerindo que o maquiavelismo seria mais bem compreendido como uma variação da psicopatia, enquanto o narcisismo se mantém o traço mais diferenciado. Meus dados não resolvem a questão, e nem poderiam: são descritivos, baseados em autorrelato e em uma amostra autosselecionada. Mas ilustram empiricamente o mesmo padrão, e em um volume considerável de respondentes.
# MAGIC
# MAGIC Vale ainda a ressalva que fiz na pergunta 3: parte da associação entre os traços pode vir de estilo de resposta, já que quem tende a concordar (ou a negar) responde de forma parecida em todos os itens, inflando as correlações. Distinguir sobreposição real de artefato de medida exigiria recursos que este conjunto de dados não oferece, como heterorrelato ou medidas objetivas de personalidade.
# MAGIC
# MAGIC Do ponto de vista da engenharia de dados, o trabalho deixou uma lição igualmente importante: decisões aparentemente técnicas têm consequências analíticas diretas. Tratar o valor 0 como ausência, e não como resposta, evitou rebaixar artificialmente todos os escores. A inversão de cinco itens alterou de forma perceptível as médias de narcisismo e psicopatia. Escolher média em vez de soma definiu a comparabilidade entre os traços. E o critério de corte determinou o que passou a ser chamado de "elevado". Nenhuma dessas escolhas é neutra, e é justamente por isso que documentá-las importa: o pipeline não entrega apenas números, entrega números com uma cadeia de decisões rastreável por trás.
# MAGIC
# MAGIC Por fim, uma observação sobre o enquadramento. Os traços aqui analisados são dimensionais e subclínicos, presentes em todas as pessoas em algum grau. Nada neste trabalho permite classificar indivíduos, e o critério de "escore elevado" descreve posição relativa dentro desta amostra, não condição clínica. Manter essa distinção explícita não é apenas um cuidado ético: é o que impede que uma análise descritiva bem construída seja lida como um diagnóstico que ela nunca foi.
# MAGIC
# MAGIC ### Limitações
# MAGIC
# MAGIC - Amostra online e autosselecionada: os resultados descrevem quem procurou o teste, não a população geral.
# MAGIC - Dados de autorrelato, sujeitos ao viés de desejabilidade social, que tende a subestimar os escores.
# MAGIC - O critério de escore elevado é relativo a esta amostra, sem valor diagnóstico.
# MAGIC - O país indica local de acesso por IP, não nacionalidade.
# MAGIC - O SD3 capta predominantemente a faceta grandiosa do narcisismo, deixando de fora a faceta vulnerável.