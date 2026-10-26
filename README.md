# tempavg-reduce — Matriz de correlação com Hadoop MapReduce

Projeto Java/Maven que calcula a **correlação de Pearson entre todos os pares de
colunas numéricas** do dataset **Forest Fires (UCI)**, usando o modelo MapReduce
do Hadoop.

Todo o código está em uma única classe: `MatrizCorrelacao`.

## Formato da entrada

Dataset Forest Fires (UCI), CSV sem cabeçalho, uma medição por linha:

```
X, Y, month, day, FFMC, DMC, DC, ISI, temp, RH, wind, rain, area
0  1     2    3    4     5    6    7    8    9   10    11    12
```

Ex.: `7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0`

`month` e `day` são categóricas e ficam de fora; as outras 11 colunas entram na
matriz (55 pares).

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
o sistema canadense de índice de risco de incêndio florestal.

## Correlação de Pearson

Resultado entre **-1 e +1**:

- perto de **+1** → sobem juntas
- perto de **-1** → uma sobe, a outra desce
- perto de **0** → sem relação linear

### O cuidado com a fórmula: somatórios aditivos

**Não** se calcula a correlação em cada nó e depois junta os resultados — isso dá
resultado errado. Trafegamos os **6 somatórios** que a fórmula precisa, que podem
ser somados em qualquer ordem/nó:

| Campo    | Significado |
|----------|-------------|
| `n`      | quantidade de pontos |
| `somaX`  | Σx |
| `somaY`  | Σy |
| `somaXY` | Σxy |
| `somaX2` | Σx² |
| `somaY2` | Σy² |

Só no fim, **uma única vez**:

```
r = (n·somaXY - somaX·somaY) / sqrt( (n·somaX2 - somaX²) · (n·somaY2 - somaY²) )
```

## Estrutura da classe `MatrizCorrelacao`

| Classe interna | Papel |
|----------------|-------|
| `Somatorios`   | Valor trafegado (`Writable`) com os 6 somatórios; sabe se `somar` a outro e calcular `pearson()` |
| `Mapeador`     | **Map**: para cada linha, emite `"colunaA x colunaB" → Somatorios de 1 ponto` para cada par de colunas (i < j) |
| `Redutor`      | **Reduce**: soma todos os `Somatorios` de um par e aplica Pearson uma única vez |

O método `run` configura o job (driver).

## Compilar

```bash
mvn clean package
```

Gera `target/tempavg-reduce.jar`.

## Executar

```bash
hadoop jar target/tempavg-reduce.jar br.pucpr.mapreduce.MatrizCorrelacao data/forestfireinput.csv saida
sort -t$'\t' -k2 -g saida/part-r-00000   # ordena por r
```

> Se rodar em modo local com JDK 17, adicione as flags de módulo do Hadoop:
> `export HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"`

## Resultados

Maiores correlações no dataset:

| Par          | r     | Leitura |
|--------------|-------|---------|
| `DMC x DC`   | 0.68  | ambos medem secura do solo orgânico (médio e longo prazo) |
| `X x Y`      | 0.54  | coordenadas do mapa — reflete a geografia do parque |
| `FFMC x ISI` | 0.53  | o ISI é calculado a partir do FFMC |
| `temp x RH`  | -0.53 | quanto mais quente, menor a umidade |
| `DC x temp`  | 0.50  | meses quentes coincidem com seca prolongada |

Como `FFMC`, `DMC`, `DC` e `ISI` são índices derivados uns dos outros, é natural
que apareçam fortemente correlacionados.
