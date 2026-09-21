# Pipeline de Dados da Tríade Sombria (SD3) no Databricks

**MVP de Engenharia de Dados — Especialização em Ciência de Dados e Analytics (PUC-Rio)**
Autora: Ligia Assis · Repositório: `sd3-lakehouse-pipeline`

> ⚠️ **Nota ética:** os traços da tríade sombria (maquiavelismo, narcisismo e psicopatia) são tratados aqui como **traços de personalidade dimensionais e subclínicos**, presentes em todas as pessoas em algum grau. Este projeto é **exclusivamente descritivo**, trabalha apenas com **dados anônimos e agregados** e **não tem qualquer finalidade diagnóstica**.

---

## Sumário

1. [Contexto de Negócio e Perguntas (Etapa 2 e 4.1)](#1-contexto-de-negócio-e-perguntas-etapa-2-e-41)
2. [Carga dos Dados (Etapa 4.2)](#2-carga-dos-dados-etapa-42)
3. [Modelagem e Catálogo de Dados (Etapa 4.3)](#3-modelagem-e-catálogo-de-dados-etapa-43)
4. [Pipeline de Dados (Etapa 4.4)](#4-pipeline-de-dados-etapa-44)
5. [Qualidade de Dados (Etapa 4.5)](#5-qualidade-de-dados-etapa-45)
6. [Análise de Dados (Etapa 4.5)](#6-análise-de-dados-etapa-45)
7. [Autoavaliação](#7-autoavaliação)
8. [Estrutura do Repositório](#8-estrutura-do-repositório)
9. [Referências](#9-referências)

---

## 1. Contexto de Negócio e Perguntas (Etapa 2 e 4.1)

### 1.1 Problema

A tríade sombria reúne três traços de personalidade socialmente aversivos — **maquiavelismo** (estilo interpessoal manipulador e estratégico), **narcisismo** (grandiosidade e busca de admiração) e **psicopatia** (impulsividade e baixa empatia). Esses traços fazem parte do espectro normal da personalidade e não devem ser lidos de forma maniqueísta ou patologizante (Coutinho, 2026).

Embora distintos, os traços se sobrepõem: a literatura descreve um núcleo comum, o **fator D**, associado à tendência de maximizar benefícios pessoais às custas de terceiros. Metanálises discutidas por Coutinho (2026) sugerem ainda que o maquiavelismo teria forte sobreposição com a psicopatia, podendo ser entendido como uma variação dela, enquanto o narcisismo se mostra o traço mais distinto dos três.

**Objetivo:** compreender como os traços da tríade sombria se distribuem e se combinam em uma amostra ampla de respondentes online do *Short Dark Triad* (SD3), identificando quais traços apresentam maior intensidade e quais tendem a coocorrer.

### 1.2 Perguntas de negócio

1. Qual dos três traços apresenta os escores médios mais altos, e como é a distribuição de cada um?
2. Que proporção dos respondentes apresenta escore elevado em cada traço?
3. Quais pares de traços apresentam maior associação entre si?
4. Quantos respondentes apresentam os três traços elevados simultaneamente, e como se distribuem os perfis com apenas um ou dois traços elevados?
5. Quais itens do questionário são mais endossados dentro de cada traço?
6. Existem diferenças nos escores entre grupos de respondentes (país de acesso ou origem do acesso)?

**Critério de "escore elevado":** o SD3 não possui ponto de corte diagnóstico oficial. Neste trabalho, considera-se elevado o escore **acima do percentil 75 da própria amostra** para cada traço. A escolha é discutida na seção de Análise.

**Hipótese prévia (registrada antes da análise):** esperava que maquiavelismo e psicopatia fossem o par mais associado.

### 1.3 Fonte dos dados

- **Origem:** Open-Source Psychometrics Project (openpsychometrics.org/_rawdata), respostas ao teste online do SD3.
- **Espelho utilizado para download:** Kaggle — `lucasgreenwell/short-dark-triad-responses`.
- **Instrumento:** SD3 (Paulhus & Jones, 2011), 27 itens, 9 por traço, respondidos em escala Likert de 5 pontos (1 = discordo; 3 = neutro; 5 = concordo).

### 1.4 Licença e consentimento

- **Licença:** a página do dataset no Kaggle declara *Database: Open Database (ODbL), Contents: Database Contents License (DbCL)*. Na prática, a base pode ser usada, modificada e compartilhada livremente, inclusive para fins acadêmicos, desde que atribuída à fonte original e mantida sob a mesma licença. A atribuição ao Open-Source Psychometrics Project consta na seção de fonte dos dados e nas referências.
- **Consentimento:** conforme o codebook que acompanha os dados, foram incluídos apenas participantes que indicaram que suas respostas eram precisas e que concordaram com o uso delas em pesquisa. Os dados são anônimos.

### 1.5 Estrutura dos dados brutos

Arquivo `data.csv`, separado por tabulação, com **18.192 registros e 29 colunas**:

| Coluna | Descrição | Tipo original |
|---|---|---|
| M1–M9 | Itens de maquiavelismo | inteiro (1–5; 0 = não respondido) |
| N1–N9 | Itens de narcisismo | inteiro (1–5; 0 = não respondido) |
| P1–P9 | Itens de psicopatia | inteiro (1–5; 0 = não respondido) |
| `country` | País inferido por geolocalização de IP (MaxMind GeoLite) | texto |
| `source` | Como o respondente chegou ao teste: 1 = página inicial do site, 2 = busca no Google, 3 = outra origem (por referenciador HTTP) | inteiro |

O download inclui também o arquivo `codebook.txt`, com o enunciado de cada item e a descrição das variáveis técnicas.

---

## 2. Carga dos Dados (Etapa 4.2)

A coleta foi automatizada via API do Kaggle (biblioteca `kagglehub`), dentro de um notebook do Databricks Free Edition. Optei pela API em vez do upload manual para que o pipeline fosse reprodutível de ponta a ponta: qualquer pessoa executa o notebook e obtém exatamente os mesmos dados, sem passos manuais não documentados.

Como o `kagglehub` grava os arquivos no armazenamento efêmero do cluster, eles são copiados em seguida para um **Volume do Unity Catalog** (`/Volumes/dark_triad/bronze/raw_files/sd3`), que é persistente.

Todo o projeto vive em um catálogo próprio, `dark_triad`, com um schema por camada da Arquitetura Medalhão (`bronze`, `silver`, `gold`).

**Script:** [`notebooks/01_ingestao.py`](notebooks/01_ingestao.py)

`[PRINT: Volume com os arquivos carregados]`

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

### 3.1 Modelo escolhido

Adotei um **Esquema Estrela** na camada gold, com duas tabelas fato (uma no grão de resposta a item, outra no grão de respondente) cercadas por três dimensões.

```
                 ┌──────────────────┐
                 │    dim_item      │
                 └────────┬─────────┘
                          │
┌──────────────────┐  ┌───┴──────────────────┐
│ dim_respondente  ├──┤  fato_resposta_item  │
└────────┬─────────┘  └──────────────────────┘
         │
┌────────┴─────────────────┐    ┌──────────────┐
│ fato_escore_respondente  ├────┤  dim_perfil  │
└──────────────────────────┘    └──────────────┘
```

`[PRINT: catálogo/linhagem no Unity Catalog]`

### 3.2 Catálogo de Dados

> As descrições abaixo também foram registradas como comentários de tabela no Unity Catalog.

**`gold.dim_item`** — os 27 itens do SD3. Linhagem: construída a partir do codebook do dataset.

| Campo | Descrição | Tipo | Domínio |
|---|---|---|---|
| id_item | Código do item | string | M1–M9, N1–N9, P1–P9 |
| traco | Traço medido | string | Maquiavelismo, Narcisismo, Psicopatia |
| texto_item | Enunciado original do item | string | texto livre |
| invertido | Item de pontuação invertida | boolean | true (N2, N6, N8, P2, P7), false |

**`gold.dim_respondente`** — um registro por respondente válido. Linhagem: `silver.sd3_respostas`; `id_respondente` gerado no pipeline; `origem_acesso` decodificada a partir de `source`.

| Campo | Descrição | Tipo | Domínio |
|---|---|---|---|
| id_respondente | Identificador sequencial gerado no pipeline | int | 1 a 17.715 |
| pais_acesso | País inferido por IP (não equivale a nacionalidade) | string | códigos ISO de 2 letras |
| id_origem | Código da origem do acesso | int | 1, 2, 3 |
| origem_acesso | Descrição da origem | string | Página inicial do site, Busca no Google, Outra origem |
| data_ingestao | Momento da carga na camada bronze | timestamp | — |

**`gold.fato_resposta_item`** — uma linha por respondente e item (478.305 linhas). Linhagem: `silver.sd3_respostas` em formato longo, com inversão aplicada.

| Campo | Descrição | Tipo | Domínio |
|---|---|---|---|
| id_respondente | FK para dim_respondente | int | 1 a 17.715 |
| id_item | FK para dim_item | string | M1–P9 |
| traco | Traço do item (redundância proposital para consultas) | string | 3 categorias |
| resposta_original | Resposta como fornecida | int | 1–5 |
| resposta_corrigida | Resposta após inversão (6 − valor) quando aplicável | int | 1–5 |

**`gold.fato_escore_respondente`** — uma linha por respondente. Linhagem: média de `resposta_corrigida` por traço; indicadores calculados sobre o percentil 75 da amostra.

| Campo | Descrição | Tipo | Domínio |
|---|---|---|---|
| id_respondente | FK para dim_respondente | int | 1 a 17.715 |
| escore_maquiavelismo | Média dos 9 itens do traço | double | 1,0–5,0 |
| escore_narcisismo | Média dos 9 itens do traço | double | 1,0–5,0 |
| escore_psicopatia | Média dos 9 itens do traço | double | 1,0–5,0 |
| elevado_maquiavelismo | Escore acima de 4,33 (P75) | boolean | true, false |
| elevado_narcisismo | Escore acima de 3,56 (P75) | boolean | true, false |
| elevado_psicopatia | Escore acima de 3,33 (P75) | boolean | true, false |
| id_perfil | FK para dim_perfil | int | 0–7 |

**`gold.dim_perfil`** — as oito combinações possíveis de traços elevados.

| Campo | Descrição | Tipo | Domínio |
|---|---|---|---|
| id_perfil | Código da combinação (maq×4 + narc×2 + psic) | int | 0–7 |
| descricao_perfil | Descrição da combinação | string | de "Nenhum traço elevado" a "Tríade completa" |
| qtd_tracos_elevados | Número de traços elevados | int | 0–3 |

---

## 4. Pipeline de Dados (Etapa 4.4)

O pipeline segue a **Arquitetura Medalhão**, ramificado em um notebook por etapa, o que mantém cada responsabilidade isolada e facilita reexecutar apenas o trecho necessário:

| Notebook | Camada | O que faz |
|---|---|---|
| [`01_ingestao`](notebooks/01_ingestao.py) | — → Volume | Baixa os dados via API do Kaggle e persiste no Volume do Unity Catalog |
| [`02_bronze`](notebooks/02_bronze.py) | Volume → bronze | Lê o arquivo como veio (todas as colunas como texto) e adiciona metadados de controle |
| [`03_qualidade`](notebooks/03_qualidade.py) | bronze | Diagnostica completude, consistência, unicidade, acurácia e outliers, sem alterar dados |
| [`04_silver`](notebooks/04_silver.py) | bronze → silver | Tipagem, exclusões documentadas e geração do identificador de respondente |
| [`05_gold`](notebooks/05_gold.py) | silver → gold | Dimensões e fatos, inversão dos itens, escores e perfis |
| [`06_analise`](notebooks/06_analise.py) | gold | Consultas que respondem às perguntas de negócio |

As seis etapas também foram orquestradas como um **Job** no Databricks, com dependência encadeada entre as tarefas, de modo que o fluxo possa ser executado de ponta a ponta em uma única acionada.

`[PRINT: diagrama do Job com as tarefas encadeadas]`

### Principais transformações

- **Metadados de ingestão (bronze):** acrescentei `_data_ingestao` e `_arquivo_origem` a cada registro, preservando a linhagem sem alterar o conteúdo original.
- **Tipagem (silver):** converti os 27 itens e `source` para inteiro. Manter tudo como texto na bronze evita que o Spark descarte silenciosamente valores inesperados na entrada.
- **Exclusões (silver):** removi respondentes com itens em branco e com resposta uniforme (detalhes na seção de qualidade).
- **Formato longo (gold):** transformei as 27 colunas de itens em linhas com `stack`, permitindo junção com `dim_item` e análise item a item.
- **Inversão de itens (gold):** apliquei `6 − resposta` em N2, N6, N8, P2 e P7, para que valores altos sempre indiquem mais do traço. Sem essa correção, os escores de narcisismo e psicopatia ficariam distorcidos — na prática, a psicopatia caiu de 2,98 para 2,81 e o narcisismo subiu de 3,03 para 3,07 após a correção. Mantive `resposta_original` e `resposta_corrigida` lado a lado para tornar a conta auditável.
- **Escores (gold):** usei a média dos 9 itens, e não a soma, para manter o resultado na escala original (1 a 5) e tornar os três traços diretamente comparáveis.

`[PRINTS: tabelas persistidas em cada camada]`

---

## 5. Qualidade de Dados (Etapa 4.5)

| Dimensão | Achado | Tratamento na Silver |
|---|---|---|
| Completude | 452 respondentes (2,5%) com ao menos um item em branco | Excluídos |
| Consistência | Nenhum valor fora do domínio esperado | Nenhum tratamento |
| Unicidade | 59 registros com respostas idênticas a outras | Mantidos |
| Acurácia | 31 respondentes com o mesmo valor nos 27 itens (25 removidos nesta etapa; 6 já haviam saído na completude) | Excluídos |
| Outliers | 136 (maquiavelismo), 291 (narcisismo) e 38 (psicopatia) escores abaixo do limite inferior | Mantidos |

**Retenção final: 17.715 registros (97,38% da base original).**

**Completude.** Não encontrei valores nulos, mas identifiquei o valor 0 em 452 respondentes. Como a escala do SD3 varia de 1 a 5, interpreto o 0 como item não respondido. A maioria deixou apenas um item em branco, mas 10 registros tinham 17 ou mais itens faltantes, incluindo 6 questionários integralmente vazios. Excluí todos os respondentes com qualquer item em branco: a perda é pequena e garante escores calculados sobre os 9 itens de cada traço, sem recorrer a imputação.

**Consistência.** Todos os itens variam entre 0 e 5, sem nenhum valor fora do esperado, o que indica que o formulário de origem restringia as opções de resposta. A exclusão dos zeros já resolve o único valor fora da escala.

**Unicidade.** Como o dataset não possui identificador de respondente, avaliei duplicidade pela repetição exata do conjunto de respostas: 18.133 combinações distintas em 18.192 registros. Ao inspecionar os 59 repetidos, verifiquei que são padrões extremos (todos os itens 5, 1, 3 ou 0), ou seja, coincidência entre respostas uniformes, e não duplicação técnica. Removê-los descartaria respostas legítimas.

**Acurácia.** Encontrei 31 respondentes que marcaram o mesmo valor nos 27 itens: 12 responderam tudo 5, 6 tudo 1, 6 tudo 0, 5 tudo 3 e 2 tudo 4. Padrões uniformes indicam baixo engajamento com o conteúdo dos itens, comprometendo a validade individual da resposta, e por isso foram excluídos. Escalas de traços sombrios são reconhecidamente sensíveis ao viés de desejabilidade social e podem ser falseadas de forma intencional (Coutinho, 2026) — vale notar, porém, que aqui o padrão de resposta máxima foi o dobro do de resposta mínima.

**Outliers.** Sobre os escores por traço, os casos extremos estão todos abaixo do limite inferior; não há outliers superiores, já que os limites calculados ultrapassam o máximo da escala. São escores baixos e plenamente possíveis, não erros de medida, então foram mantidos: descartá-los enviesaria a distribuição justamente na cauda que interessa descrever.

`[PRINTS: consultas de qualidade]`

---

## 6. Análise de Dados (Etapa 4.5)

### Pergunta 1 — Intensidade e distribuição dos traços

| Traço | Média | Mediana | Desvio-padrão |
|---|---|---|---|
| Maquiavelismo | 3,71 | 3,78 | 0,79 |
| Narcisismo | 3,07 | 3,00 | 0,77 |
| Psicopatia | 2,81 | 2,78 | 0,81 |

O maquiavelismo lidera com folga — quase um ponto inteiro acima da psicopatia em uma escala de cinco. Uma leitura possível é que os três traços não são igualmente fáceis de admitir: os itens de maquiavelismo descrevem estratégia interpessoal, que pode ser lida como prudência ou inteligência social, enquanto os de psicopatia exigem admitir crueldade e descontrole, socialmente muito mais custoso. Isso é coerente com a discussão sobre desejabilidade social nessas escalas (Coutinho, 2026).

### Pergunta 2 — Proporção com escore elevado

Como o critério é o percentil 75, cerca de um quarto da amostra é classificado como elevado em cada traço, por construção. O achado relevante está no **valor exigido por cada corte**: 4,33 para maquiavelismo, 3,56 para narcisismo e 3,33 para psicopatia. Entrar no quarto superior de maquiavelismo exige concordar fortemente com quase todos os itens; no caso da psicopatia, basta pontuação próxima do ponto neutro. Um corte absoluto idêntico para os três traços classificaria quase ninguém como elevado em psicopatia e distorceria a comparação — daí a opção por um critério relativo e explicitamente descritivo.

### Pergunta 3 — Associação entre pares de traços

| Par | Correlação de Pearson |
|---|---|
| Maquiavelismo × Psicopatia | **0,67** |
| Narcisismo × Psicopatia | 0,57 |
| Maquiavelismo × Narcisismo | 0,51 |

O par maquiavelismo–psicopatia é o mais associado, confirmando a hipótese registrada antes da análise. O resultado converge com as metanálises discutidas por Coutinho (2026), que apontam o maquiavelismo como possível variação da psicopatia e o narcisismo como o traço mais distinto — aqui, também, suas duas correlações são as mais baixas. O fato de as três correlações serem substanciais dá apoio empírico à noção de fator D.

Em termos de coocorrência: entre os respondentes com maquiavelismo elevado, **57,6%** também apresentam psicopatia elevada e 53,1% apresentam narcisismo elevado. Como o acaso produziria cerca de 25%, todos os pares ocorrem muito acima do esperado.

**Ressalva de método:** correlação não é evidência de que os construtos sejam o mesmo. Parte da associação pode vir de estilo de resposta — quem tende a concordar (ou a negar) responde de forma parecida em todos os itens, inflando as correlações.

### Pergunta 4 — Perfis de combinação

| Perfil | Respondentes | % |
|---|---|---|
| Nenhum traço elevado | 10.273 | 57,99 |
| Apenas narcisismo | 1.484 | 8,38 |
| **Tríade completa** | **1.451** | **8,19** |
| Apenas psicopatia | 1.245 | 7,03 |
| Apenas maquiavelismo | 1.042 | 5,88 |
| Narcisismo + psicopatia | 884 | 4,99 |
| Maquiavelismo + psicopatia | 755 | 4,26 |
| Maquiavelismo + narcisismo | 581 | 3,28 |

A tríade completa é o **terceiro perfil mais frequente**, à frente de qualquer combinação de exatamente dois traços. Se os três fossem independentes entre si, ela seria o perfil mais raro. O padrão observado indica o contrário: quando alguém pontua alto, tende a pontuar alto em tudo.

### Pergunta 5 — Itens mais endossados

Os itens mais endossados revelam que o que as pessoas admitem com facilidade não é a conduta em si, mas a **leitura de mundo** que a justifica.

No maquiavelismo, o topo é ocupado pelos itens mais impessoais: guardar o que os outros não precisam saber (M7, média 4,39, com 89% de concordância), não contar segredos (M1, 4,18) e acreditar que a maioria pode ser manipulada (M9, 4,08). O último colocado é justamente o que explicita o autointeresse (M8, 3,17).

Na psicopatia, o item mais endossado surpreende: admitir que se pode ser mau com os outros (P5, 3,72) fica isolado no topo, muito acima do segundo colocado. Já o item menos endossado de todo o instrumento é ser visto como descontrolado (P4, 2,32). Admitir crueldade, portanto, não é tabu; perder o controle é — o que se preserva na autoimagem parece ser a agência, não a bondade.

No narcisismo, passam os itens socialmente legítimos, como conhecer pessoas importantes (N5, 3,51) e fazer questão do respeito merecido (N9, 3,45); a grandiosidade declarada fica no fim (N4, 2,68; N3, 2,65).

Os itens invertidos se comportam de modo coerente: após a correção, N6 (2,90), N2 (2,88) e P2 (2,79) ficam abaixo da média de seus traços, que é exatamente o efeito que itens reversos buscam captar.

### Pergunta 6 — Diferenças entre grupos

As médias por país de acesso variam pouco (maquiavelismo entre 3,42 e 3,92; narcisismo entre 2,66 e 3,22; psicopatia entre 2,56 e 2,88), e os quatro países com maior volume — Estados Unidos (8.996), Reino Unido (2.724), Canadá (1.182) e Austrália (746) — praticamente não se distinguem entre si. Essa variação **não sustenta conclusões sobre populações nacionais**: a amostra é autosselecionada, o campo indica local de acesso por IP e não nacionalidade, e os volumes são muito desiguais.

A comparação por origem do acesso é mais segura, por não envolver grupo social. Quem chegou por busca no Google (2.639) apresenta as médias mais altas nos três traços (3,78 / 3,22 / 2,92), acima de quem chegou pela página inicial (9.646; 3,72 / 3,03 / 2,80) ou por outra origem (5.430; 3,67 / 3,07 / 2,79). As diferenças são pequenas, mas consistentes nos três traços — possivelmente autosseleção por motivação, já que quem busca ativamente pelo teste pode ter interesse específico no tema.

`[PRINTS: resultados das consultas]`

### Discussão geral

Reunindo as respostas, chego a duas conclusões complementares: **os três traços se distinguem com clareza em intensidade, mas são pouco independentes entre si**.

Em intensidade, a ordem é nítida (maquiavelismo, narcisismo, psicopatia) e a análise item a item mostrou que essa hierarquia acompanha menos a intensidade do traço e mais o custo social de admiti-lo. Em combinação, os traços caminham juntos muito acima do acaso: correlações substanciais, destaque para maquiavelismo–psicopatia (0,67) e a tríade completa entre os perfis mais comuns.

Esse conjunto dialoga com o debate atual da área sobre a real independência dos três construtos. Os dados não resolvem a questão — são descritivos, de autorrelato e de amostra autosselecionada — mas ilustram o mesmo padrão descrito nas metanálises, com volume considerável de respondentes.

Do ponto de vista da engenharia de dados, o trabalho deixou uma lição igualmente importante: decisões aparentemente técnicas têm consequências analíticas diretas. Tratar o 0 como ausência evitou rebaixar todos os escores; a inversão de cinco itens alterou as médias de dois traços; média em vez de soma definiu a comparabilidade; e o critério de corte determinou o que se chama de "elevado". O pipeline não entrega apenas números, entrega números com uma cadeia de decisões rastreável.

### Limitações

- Amostra online e autosselecionada: os resultados descrevem quem procurou o teste, não a população geral.
- Dados de autorrelato, sujeitos ao viés de desejabilidade social, que tende a subestimar os escores.
- O critério de escore elevado é relativo a esta amostra, sem valor diagnóstico.
- O país indica local de acesso por IP, não nacionalidade.
- O SD3 capta predominantemente a faceta grandiosa do narcisismo, deixando de fora a faceta vulnerável.
- Parte da associação entre traços pode refletir estilo de resposta, e não sobreposição real dos construtos.

---

## 7. Autoavaliação

### Objetivos atingidos

Considero plenamente respondida a pergunta central do trabalho, sobre a associação entre os traços: as correlações foram calculadas, a hierarquia entre os pares ficou clara e o resultado pôde ser confrontado com a hipótese que eu havia registrado antes da análise e com o que a literatura recente discute. As perguntas sobre perfis de combinação e sobre os itens mais endossados também foram respondidas de forma satisfatória.

As perguntas sobre distribuição e sobre proporção de escores elevados considero respondidas apenas parcialmente — não por falha na execução, mas por um limite do próprio dado. Descrever como os escores se distribuem em uma amostra não é o mesmo que dizer o que aquele escore significa para uma pessoa específica, e é justamente essa passagem que o SD3, aplicado online e sem contexto clínico, não permite fazer. A sexta pergunta, sobre diferenças entre grupos, foi respondida tecnicamente, mas concluí que os dados disponíveis não sustentam interpretação sobre populações nacionais; mantive a análise no relatório porque a própria impossibilidade de concluir é um achado relevante.

### Dificuldades encontradas

A etapa mais difícil foi a análise, e não a construção técnica do pipeline. Interpretar os resultados exigiu recorrer à literatura atual sobre a tríade sombria para não incorrer em leituras que os dados não autorizam, e esse trabalho de fundamentação consumiu mais tempo e atenção do que as transformações em si.

A decisão mais difícil foi definir o que contaria como "escore elevado". O SD3 não possui pontos de corte clínicos, e qualquer critério que eu adotasse seria uma escolha minha, com consequências diretas sobre todos os resultados seguintes. Optei pelo percentil 75 da própria amostra, o que coloca cerca de um quarto dos respondentes como elevados em cada traço por construção. O achado relevante não está no percentual, e sim no valor de escore que cada corte exige: 4,33 para maquiavelismo, 3,56 para narcisismo e 3,33 para psicopatia. Estar entre os 25% mais altos significa, portanto, coisas bem diferentes dependendo do traço — em maquiavelismo é preciso concordar fortemente com quase todos os itens, enquanto em psicopatia basta uma pontuação próxima do ponto neutro. Um corte absoluto idêntico para os três, como escore acima de 4, classificaria quase ninguém como elevado em psicopatia e distorceria a comparação.

Assumi esse critério como relativo e descritivo, e essa é ao mesmo tempo a principal limitação do trabalho: os perfis descrevem posições dentro desta amostra, não condições clínicas. Tratar esses limites como diagnósticos seria exatamente o tipo de leitura patologizante que a literatura da área recomenda evitar, já que os traços sombrios são dimensionais e estão presentes em todas as pessoas em algum grau (Coutinho, 2026).

### Sobre o processo

A definição do tema foi mais trabalhosa do que eu esperava. Comecei explorando dados de aviação e, ao perceber que o assunto não me mobilizava, migrei para a tríade sombria, buscando algo mais próximo da neuropsicologia. A troca custou tempo, mas foi acertada: trabalhar com um instrumento que eu conheço permitiu tomar decisões técnicas com fundamento — como identificar os itens de pontuação invertida e compreender por que ignorá-los distorceria os escores — em vez de apenas executar transformações.

### Trabalhos futuros

- **Agente de IA** sobre as tabelas gold, permitindo consultar os resultados em linguagem natural e aproveitando o catálogo de dados já documentado.
- **Comparação com o Big Five**, usando outro conjunto do mesmo acervo, para situar os traços sombrios em relação a um modelo amplamente consolidado.
- **Revisão de literatura mais sistemática**, ampliando a fundamentação da discussão e permitindo confrontar os achados com um conjunto maior de estudos.

---

## 8. Estrutura do Repositório

```
sd3-lakehouse-pipeline/
├── README.md
├── notebooks/
│   ├── 01_ingestao.py
│   ├── 02_bronze.py
│   ├── 03_qualidade.py
│   ├── 04_silver.py
│   ├── 05_gold.py
│   └── 06_analise.py
└── img/
    └── (prints de evidência)
```

Os dados não estão versionados neste repositório; são obtidos automaticamente pelo notebook de ingestão.

---

## 9. Referências

- Coutinho, T. V. (2026). Personalidades sombrias: o que dizem as metanálises. In: Associação Brasileira de Psiquiatria; A. G. Silva, & L. F. Malloy-Diniz (Orgs.). *PRONEUROPSI Programa de Atualização em Neuropsicologia: Ciclo 1* (pp. 119-46). Artmed Panamericana. (Sistema de Educação Continuada a Distância, v. 2).
- Paulhus, D. L., & Jones, D. N. (2011, janeiro). *Introducing a short measure of the Dark Triad.* Pôster apresentado no encontro da Society for Personality and Social Psychology, San Antonio.
- Open-Source Psychometrics Project. *Raw data from online personality tests.* openpsychometrics.org/_rawdata
- Databricks. *Medallion Architecture* e *Unity Catalog.* docs.databricks.com
