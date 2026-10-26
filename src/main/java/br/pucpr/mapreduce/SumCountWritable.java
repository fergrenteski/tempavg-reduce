package br.pucpr.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Tipo de valor que trafega entre Map -> (Combiner) -> Reduce.
 *
 * A CHAVE do exercicio esta aqui: em vez de mandar uma media (ou uma unica
 * temperatura), mandamos o par:
 *
 *      soma      -> numerador  (somatorio das temperaturas)
 *      contagem  -> divisor    (quantidade de temperaturas somadas)
 *
 * Assim a agregacao e associativa: podemos somar somas e somar contagens em
 * qualquer ordem / em qualquer no, e so DEPOIS, no final, fazer soma/contagem.
 *
 * Se mandassemos a media de cada no e depois tirassemos a media das medias,
 * o resultado ficaria ERRADO, porque medias de conjuntos com tamanhos
 * diferentes nao podem ser simplesmente re-mediadas.
 */
public class SumCountWritable implements Writable {

    private double sum;    // somatorio das temperaturas (numerador)
    private long count;    // quantidade de temperaturas   (divisor)

    public SumCountWritable() {
        // Construtor vazio obrigatorio para o Hadoop (de)serializar.
    }

    public SumCountWritable(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public void set(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    /** Media ponderada correta: somatorio total / quantidade total. */
    public double average() {
        return count == 0 ? 0.0 : sum / count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sum = in.readDouble();
        this.count = in.readLong();
    }

    @Override
    public String toString() {
        return sum + "\t" + count;
    }
}
