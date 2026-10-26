#!/usr/bin/env python3
"""
MATRIZ DE CORRELACAO DE PEARSON entre todas as colunas numericas, par a par,
rodando no Hadoop via Hadoop Streaming.

O Hadoop Streaming conversa com o script por stdin/stdout:
  - Mapeador: le linhas do CSV e escreve "chave<TAB>valor".
  - O Hadoop ordena por chave (shuffle/sort).
  - Redutor: recebe as linhas ja agrupadas por chave e escreve o resultado.

Mesma ideia da versao Java: NAO se calcula a correlacao em cada no.
Trafegamos os 6 somatorios (quantidade, soma_a, soma_b, soma_a_vezes_b, ...), que sao
aditivos, e so no fim aplicamos Pearson uma unica vez.

Uso:
  python3 matriz_correlacao.py mapear   < entrada.csv
  python3 matriz_correlacao.py reduzir  < saida_ordenada_do_mapeador
"""
import math
import sys

# Colunas numericas do dataset (mes=2 e dia_semana=3 sao categoricas, ficam de fora).
INDICES_COLUNAS = [0, 1, 4, 5, 6, 7, 8, 9, 10, 11, 12]
NOMES_COLUNAS = [
    "coord_x",                  # X
    "coord_y",                  # Y
    "umidade_combustivel_fino", # FFMC
    "umidade_materia_organica", # DMC
    "indice_seca",              # DC
    "indice_propagacao",        # ISI
    "temperatura",              # temp
    "umidade_relativa",         # RH
    "vento",                    # wind
    "chuva",                    # rain
    "area_queimada",            # area
]


def mapear(entrada, saida):
    """MAP: para cada linha, emite um ponto para cada par de colunas (coluna A, coluna B)."""
    for linha in entrada:
        campos_da_linha = linha.strip().split(",")
        try:
            valores_da_linha = [float(campos_da_linha[indice]) for indice in INDICES_COLUNAS]
        except (IndexError, ValueError):
            continue  # cabecalho/linha invalida

        quantidade_colunas = len(valores_da_linha)
        for posicao_a in range(quantidade_colunas):
            for posicao_b in range(posicao_a + 1, quantidade_colunas):
                valor_a = valores_da_linha[posicao_a]
                valor_b = valores_da_linha[posicao_b]
                nome_do_par = NOMES_COLUNAS[posicao_a] + " x " + NOMES_COLUNAS[posicao_b]

                # Somatorios de 1 unico ponto: quantidade, soma_a, soma_b, soma_a_vezes_b, soma_a_quadrado, soma_b_quadrado
                somatorios = [1, valor_a, valor_b, valor_a * valor_b, valor_a ** 2, valor_b ** 2]
                saida.write(nome_do_par + "\t" + ",".join(map(str, somatorios)) + "\n")


def calcular_pearson(quantidade, soma_a, soma_b, soma_a_vezes_b, soma_a_quadrado, soma_b_quadrado):
    numerador = quantidade * soma_a_vezes_b - soma_a * soma_b
    variacao_a = quantidade * soma_a_quadrado - soma_a ** 2
    variacao_b = quantidade * soma_b_quadrado - soma_b ** 2
    denominador = math.sqrt(variacao_a * variacao_b)
    return 0.0 if denominador == 0 else numerador / denominador


def reduzir(entrada, saida):
    """REDUCE: soma os somatorios de cada par e aplica Pearson uma unica vez.

    O Hadoop entrega as linhas ordenadas por chave, entao basta detectar
    quando o nome do par muda.
    """
    par_em_andamento = None
    somatorios_do_par = [0.0] * 6

    for linha in entrada:
        nome_do_par, texto_somatorios = linha.rstrip("\n").split("\t")
        somatorios_da_linha = [float(numero) for numero in texto_somatorios.split(",")]

        if nome_do_par != par_em_andamento:
            if par_em_andamento is not None:
                saida.write(f"{par_em_andamento}\t{calcular_pearson(*somatorios_do_par)}\n")
            par_em_andamento = nome_do_par
            somatorios_do_par = [0.0] * 6

        somatorios_do_par = [
            acumulado + da_linha
            for acumulado, da_linha in zip(somatorios_do_par, somatorios_da_linha)
        ]

    if par_em_andamento is not None:
        saida.write(f"{par_em_andamento}\t{calcular_pearson(*somatorios_do_par)}\n")


if __name__ == "__main__":
    etapas = {"mapear": mapear, "reduzir": reduzir}
    if len(sys.argv) != 2 or sys.argv[1] not in etapas:
        sys.exit("Uso: matriz_correlacao.py mapear|reduzir")
    etapas[sys.argv[1]](sys.stdin, sys.stdout)
