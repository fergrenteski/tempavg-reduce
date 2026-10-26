package br.pucpr.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MATRIZ DE CORRELACAO DE PEARSON entre TODAS as colunas numericas, par a par.
 *
 * NAO se calcula a correlacao em cada no e depois a "media" delas.
 * Trafegamos os 6 somatorios que a formula precisa:
 *
 *      n, somaX, somaY, somaXY, somaX2, somaY2
 *
 * Eles sao aditivos (podem ser somados em qualquer no / ordem). So no FIM,
 * uma unica vez, aplicamos:
 *
 *      r = (n*somaXY - somaX*somaY) / sqrt( (n*somaX2 - somaX^2) * (n*somaY2 - somaY^2) )
 *
 * Resultado em -1..+1: perto de +1 sobem juntas, perto de -1 uma sobe e a outra desce.
 *
 * Uso: hadoop jar tempavg-reduce.jar br.pucpr.mapreduce.MatrizCorrelacao <entrada> <saida>
 */
public class MatrizCorrelacao extends Configured implements Tool {

    // Colunas numericas do dataset (month=2 e day=3 sao categoricas, ficam de fora).
    private static final int[] INDICES_COLUNAS = {0, 1, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    private static final String[] NOMES_COLUNAS = {
            "X", "Y", "FFMC", "DMC", "DC", "ISI", "temp", "RH", "wind", "rain", "area"
    };

    /** Acumulador com os 6 somatorios. Junta-se somando campo a campo. */
    public static class Somatorios implements Writable {
        long n;
        double somaX, somaY, somaXY, somaX2, somaY2;

        public Somatorios() { }

        void definirPonto(double x, double y) {
            n = 1;
            somaX = x; somaY = y; somaXY = x * y; somaX2 = x * x; somaY2 = y * y;
        }

        void somar(Somatorios outro) {
            n += outro.n;
            somaX += outro.somaX; somaY += outro.somaY; somaXY += outro.somaXY;
            somaX2 += outro.somaX2; somaY2 += outro.somaY2;
        }

        double pearson() {
            double numerador = n * somaXY - somaX * somaY;
            double denominador = Math.sqrt((n * somaX2 - somaX * somaX)
                                         * (n * somaY2 - somaY * somaY));
            return denominador == 0 ? 0.0 : numerador / denominador;
        }

        @Override public void write(DataOutput saida) throws IOException {
            saida.writeLong(n); saida.writeDouble(somaX); saida.writeDouble(somaY);
            saida.writeDouble(somaXY); saida.writeDouble(somaX2); saida.writeDouble(somaY2);
        }

        @Override public void readFields(DataInput entrada) throws IOException {
            n = entrada.readLong(); somaX = entrada.readDouble(); somaY = entrada.readDouble();
            somaXY = entrada.readDouble(); somaX2 = entrada.readDouble(); somaY2 = entrada.readDouble();
        }
    }

    /** MAP: para cada linha, emite um Somatorios de 1 ponto para cada par de colunas (i < j). */
    public static class Mapeador extends Mapper<LongWritable, Text, Text, Somatorios> {
        private final Text chave = new Text();
        private final Somatorios ponto = new Somatorios();

        @Override protected void map(LongWritable deslocamento, Text linha, Context contexto)
                throws IOException, InterruptedException {
            String[] campos = linha.toString().trim().split(",");

            // Le as colunas numericas desta linha; pula a linha toda se alguma falhar.
            double[] valores = new double[INDICES_COLUNAS.length];
            for (int c = 0; c < INDICES_COLUNAS.length; c++) {
                if (INDICES_COLUNAS[c] >= campos.length) return;
                try {
                    valores[c] = Double.parseDouble(campos[INDICES_COLUNAS[c]].trim());
                } catch (NumberFormatException e) {
                    return; // cabecalho/linha invalida
                }
            }

            for (int i = 0; i < INDICES_COLUNAS.length; i++) {
                for (int j = i + 1; j < INDICES_COLUNAS.length; j++) {
                    chave.set(NOMES_COLUNAS[i] + " x " + NOMES_COLUNAS[j]);
                    ponto.definirPonto(valores[i], valores[j]);
                    contexto.write(chave, ponto);
                }
            }
        }
    }

    /** REDUCE: soma todos os Somatorios de um par e aplica Pearson uma unica vez. */
    public static class Redutor extends Reducer<Text, Somatorios, Text, Text> {
        @Override protected void reduce(Text par, Iterable<Somatorios> parciais, Context contexto)
                throws IOException, InterruptedException {
            Somatorios total = new Somatorios();
            for (Somatorios parcial : parciais) total.somar(parcial);
            contexto.write(par, new Text(String.valueOf(total.pearson())));
        }
    }

    @Override public int run(String[] argumentos) throws Exception {
        if (argumentos.length != 2) {
            System.err.println("Uso: hadoop jar tempavg-reduce.jar "
                    + "br.pucpr.mapreduce.MatrizCorrelacao <entrada> <saida>");
            return -1;
        }
        Job tarefa = Job.getInstance(getConf(), "Matriz de correlacao");
        tarefa.setJarByClass(MatrizCorrelacao.class);
        tarefa.setMapperClass(Mapeador.class);
        tarefa.setReducerClass(Redutor.class);
        tarefa.setMapOutputKeyClass(Text.class);
        tarefa.setMapOutputValueClass(Somatorios.class);
        tarefa.setOutputKeyClass(Text.class);
        tarefa.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tarefa, new Path(argumentos[0]));
        FileOutputFormat.setOutputPath(tarefa, new Path(argumentos[1]));
        return tarefa.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] argumentos) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new MatrizCorrelacao(), argumentos));
    }
}
