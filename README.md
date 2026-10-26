# tempavg-reduce — Média de temperaturas com Hadoop MapReduce

Projeto Java/Maven que calcula a **média de temperaturas por mês** usando o
modelo MapReduce do Hadoop, sobre o dataset **Forest Fires (UCI)**.

## O cuidado com a fórmula (slide 05)

A média **não** pode ser calculada tirando a média de cada nó e depois a média
dessas médias — isso dá resultado **errado** quando os grupos têm tamanhos
diferentes. (Neste dataset os meses são bem desiguais: `aug` tem 184 registros e
`nov` tem apenas 1, então o erro seria grande.)

A solução implementada trafega um par **`(soma, contagem)`** como valor:

| Campo      | Papel        |
|------------|--------------|
| `soma`     | numerador (somatório das temperaturas) |
| `contagem` | divisor (quantidade de temperaturas)   |

Fluxo:

1. **Map** emite `mês → (temperatura, 1)`.
2. **Combiner** (opcional, mas seguro) soma as somas e as contagens — **sem dividir**.
3. **Reduce** soma tudo e faz a divisão **uma única vez**: `somaTotal / contagemTotal`.

Como a agregação é feita sobre soma e contagem, o combiner produz exatamente o
mesmo resultado do reduce — por isso é seguro ligá-lo. Se emitíssemos médias, o
combiner daria resultado errado.

## As três classes

| Classe                 | Papel   |
|------------------------|---------|
| `Main`                 | driver / configuração do job (contém o Combiner como classe interna opcional) |
| `TemperatureMapper`    | Map |
| `TemperatureReducer`   | Reduce |

`SumCountWritable` é o tipo de valor auxiliar que carrega o par `(soma, contagem)`.

## Formato da entrada

Dataset Forest Fires (UCI), CSV sem cabeçalho, uma medição por linha:

```
X, Y, month, day, FFMC, DMC, DC, ISI, temp, RH, wind, rain, area
0  1     2    3    4     5    6    7    8    9   10    11    12
```

Ex.: `7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0`

O Map usa **month** (índice 2) como chave e **temp** (índice 8) como valor.

### O que significa cada coluna

| Coluna  | Significado |
|---------|-------------|
| `X`, `Y` | Coordenadas espaciais no mapa do Parque Natural de Montesinho (Portugal), grade 1–9 × 2–9 |
| `month` | Mês (`jan` … `dec`) |
| `day`   | Dia da semana (`mon` … `sun`) |
| `FFMC`  | *Fine Fuel Moisture Code* — umidade do combustível fino da superfície (folhas, gravetos). Quanto maior, mais seco e mais fácil de **iniciar** fogo |
| `DMC`   | *Duff Moisture Code* — umidade da camada orgânica pouco compactada logo abaixo da superfície (secagem de médio prazo) |
| `DC`    | *Drought Code* — umidade da camada orgânica profunda e compacta; indica **seca de longo prazo** |
| `ISI`   | *Initial Spread Index* — velocidade esperada de **propagação** inicial do fogo (combina FFMC + vento) |
| `temp`  | Temperatura em °C |
| `RH`    | Umidade relativa do ar em % |
| `wind`  | Velocidade do vento em km/h |
| `rain`  | Chuva acumulada em mm/m² |
| `area`  | Área queimada em hectares (0 = sem área significativa queimada) |

`FFMC`, `DMC`, `DC` e `ISI` são componentes do **FWI** (*Fire Weather Index*),
o sistema canadense de índice de risco de incêndio florestal. Por isso `DMC × DC`
e `FFMC × ISI` aparecem entre as maiores correlações: são índices derivados uns
dos outros.

## Compilar

```bash
mvn clean package
```

Gera `target/tempavg-reduce.jar`.

## Executar

Com um cluster/instalação do Hadoop:

```bash
hadoop jar target/tempavg-reduce.jar br.pucpr.mapreduce.Main data/forestfireinput.csv saida
cat saida/part-r-00000
```

Saída esperada (média correta de temperatura por mês):

```
apr  12.044444444444444
aug  21.63152173913044
dec  4.522222222222222
feb  9.635
jan  5.25
jul  22.109375
jun  20.49411764705882
mar  13.083333333333334
may  14.65
nov  11.8
oct  17.093333333333334
sep  19.612209302325577
```

> Se rodar em modo local com JDK 17, adicione as flags de módulo do Hadoop:
> `export HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"`

## Correlação entre variáveis (`CorrelationMain`)

Mede a **correlação de Pearson** entre duas colunas — por padrão `temp` × `RH`
(umidade). Resultado entre **-1 e +1**:

- perto de **+1** → sobem juntas
- perto de **-1** → uma sobe, a outra desce
- perto de **0** → sem relação linear

### Mesma ideia da média: somatórios aditivos

Não se calcula a correlação por nó e depois junta. Trafegamos os **6 somatórios**
que a fórmula precisa (`n, Σx, Σy, Σxy, Σx², Σy²`), que podem ser somados em
qualquer ordem/nó. Só no fim, **uma única vez**:

```
r = (n·Σxy - Σx·Σy) / sqrt( (n·Σx² - Σx²) · (n·Σy² - Σy²) )
```

### Rodar

```bash
hadoop jar target/tempavg-reduce.jar br.pucpr.mapreduce.CorrelationMain data/forestfireinput.csv saida-corr
cat saida-corr/part-r-00000
```

Saída esperada:

```
temp-RH  -0.5273904...
```

Ou seja: quanto mais quente, menor a umidade (correlação negativa moderada).
Para correlacionar outras colunas, troque `X_INDEX` / `Y_INDEX` em
`CorrelationMain`.

### Matriz: todas as variáveis (`CorrelationMatrixMain`)

Calcula Pearson para **todos os pares** de colunas numéricas de uma vez.
Reaproveita o `Stats` e o `Reduce` do `CorrelationMain`; só o Map muda, emitindo
um par para cada combinação `(i, j)`.

```bash
hadoop jar target/tempavg-reduce.jar br.pucpr.mapreduce.CorrelationMatrixMain data/forestfireinput.csv saida-matriz
sort -t$'\t' -k2 -g saida-matriz/part-r-00000   # ordena por r
```

Maiores correlações no dataset: `DMC x DC 0.68`, `X x Y 0.54`, `FFMC x ISI 0.53`
e a já conhecida `temp x RH -0.53`.
