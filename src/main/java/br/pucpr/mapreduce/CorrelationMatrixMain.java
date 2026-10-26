package br.pucpr.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * MATRIZ DE CORRELACAO: Pearson entre TODAS as colunas numericas, par a par.
 *
 * Mesma ideia do CorrelationMain, so que o Map emite um par para cada
 * combinacao de colunas (i < j). Reaproveita Stats e Reduce de CorrelationMain
 * (a formula e a agregacao sao identicas).
 *
 * Uso: hadoop jar tempavg-reduce.jar br.pucpr.mapreduce.CorrelationMatrixMain <entrada> <saida>
 */
public class CorrelationMatrixMain extends Configured implements Tool {

    // Colunas numericas do dataset (month=2 e day=3 sao categoricas, ficam de fora).
    private static final int[] COLS = {0, 1, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    private static final String[] NOMES = {
            "X", "Y", "FFMC", "DMC", "DC", "ISI", "temp", "RH", "wind", "rain", "area"
    };

    public static class Map extends Mapper<LongWritable, Text, Text, CorrelationMain.Stats> {
        private final Text key = new Text();
        private final CorrelationMain.Stats out = new CorrelationMain.Stats();

        @Override protected void map(LongWritable k, Text value, Context ctx)
                throws IOException, InterruptedException {
            String[] f = value.toString().trim().split(",");

            // Le as colunas numericas desta linha; pula a linha toda se alguma falhar.
            double[] v = new double[COLS.length];
            for (int c = 0; c < COLS.length; c++) {
                if (COLS[c] >= f.length) return;
                try {
                    v[c] = Double.parseDouble(f[COLS[c]].trim());
                } catch (NumberFormatException e) {
                    return; // cabecalho/linha invalida
                }
            }

            // Emite um Stats para cada par de colunas (i < j).
            for (int i = 0; i < COLS.length; i++) {
                for (int j = i + 1; j < COLS.length; j++) {
                    key.set(NOMES[i] + " x " + NOMES[j]);
                    out.set(v[i], v[j]);
                    ctx.write(key, out);
                }
            }
        }
    }

    @Override public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: hadoop jar tempavg-reduce.jar "
                    + "br.pucpr.mapreduce.CorrelationMatrixMain <entrada> <saida>");
            return -1;
        }
        Job job = Job.getInstance(getConf(), "Matriz de correlacao");
        job.setJarByClass(CorrelationMatrixMain.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(CorrelationMain.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CorrelationMain.Stats.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CorrelationMatrixMain(), args));
    }
}
