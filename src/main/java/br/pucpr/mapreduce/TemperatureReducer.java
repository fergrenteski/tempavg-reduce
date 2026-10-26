package br.pucpr.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * REDUCE
 *
 * Recebe, para cada chave, todos os pares (soma, contagem) vindos do Map
 * (e opcionalmente ja pre-agregados pelo Combiner).
 *
 * Estrategia CORRETA:
 *   1) soma TODAS as somas         -> somaTotal   (numerador)
 *   2) soma TODAS as contagens     -> contagemTotal (divisor)
 *   3) media = somaTotal / contagemTotal   <-- divisao feita UMA UNICA VEZ, no fim
 *
 * Nunca tiramos "media das medias".
 */
public class TemperatureReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {

    private final DoubleWritable media = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
            throws IOException, InterruptedException {

        double somaTotal = 0.0;   // numerador acumulado
        long contagemTotal = 0L;  // divisor acumulado

        for (SumCountWritable v : values) {
            somaTotal += v.getSum();
            contagemTotal += v.getCount();
        }

        if (contagemTotal == 0L) {
            return; // seguranca: sem dados para esta chave
        }

        media.set(somaTotal / contagemTotal); // divisao unica e final
        context.write(key, media);
    }
}
