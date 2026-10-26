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
 * CORRELACAO DE PEARSON entre duas colunas (temp x RH) via MapReduce.
 *
 * Mesma ideia da media: NAO se calcula correlacao por no e depois a "media" delas.
 * Em vez disso trafegamos os 6 somatorios que a formula precisa:
 *
 *      n, Sx, Sy, Sxy, Sxx, Syy
 *
 * Eles sao aditivos (podem ser somados em qualquer no / ordem). So no FIM,
 * uma unica vez, aplicamos:
 *
 *      r = (n*Sxy - Sx*Sy) / sqrt( (n*Sxx - Sx^2) * (n*Syy - Sy^2) )
 *
 * Resultado em -1..+1: perto de +1 sobem juntas, perto de -1 uma sobe a outra desce.
 *
 * Uso: hadoop jar tempavg-reduce.jar br.pucpr.mapreduce.CorrelationMain <entrada> <saida>
 */
public class CorrelationMain extends Configured implements Tool {

    private static final int X_INDEX = 8; // coluna "temp"
    private static final int Y_INDEX = 9; // coluna "RH" (umidade)

    /** Acumulador com os 6 somatorios. Soma-se somando campo a campo. */
    public static class Stats implements Writable {
        long n;
        double sx, sy, sxy, sxx, syy;

        public Stats() { }

        void set(double x, double y) {
            n = 1;
            sx = x; sy = y; sxy = x * y; sxx = x * x; syy = y * y;
        }

        void add(Stats o) {
            n += o.n; sx += o.sx; sy += o.sy; sxy += o.sxy; sxx += o.sxx; syy += o.syy;
        }

        @Override public void write(DataOutput out) throws IOException {
            out.writeLong(n); out.writeDouble(sx); out.writeDouble(sy);
            out.writeDouble(sxy); out.writeDouble(sxx); out.writeDouble(syy);
        }

        @Override public void readFields(DataInput in) throws IOException {
            n = in.readLong(); sx = in.readDouble(); sy = in.readDouble();
            sxy = in.readDouble(); sxx = in.readDouble(); syy = in.readDouble();
        }
    }

    /** MAP: cada linha vira um Stats de 1 ponto, sob uma chave unica. */
    public static class Map extends Mapper<LongWritable, Text, Text, Stats> {
        private final Text key = new Text("temp-RH");
        private final Stats out = new Stats();

        @Override protected void map(LongWritable k, Text value, Context ctx)
                throws IOException, InterruptedException {
            String[] f = value.toString().trim().split(",");
            if (f.length <= Y_INDEX) return;
            try {
                out.set(Double.parseDouble(f[X_INDEX].trim()),
                        Double.parseDouble(f[Y_INDEX].trim()));
            } catch (NumberFormatException e) {
                return; // cabecalho/linha invalida
            }
            ctx.write(key, out);
        }
    }

    /** REDUCE: soma todos os Stats e aplica Pearson uma unica vez. */
    public static class Reduce extends Reducer<Text, Stats, Text, Text> {
        @Override protected void reduce(Text key, Iterable<Stats> values, Context ctx)
                throws IOException, InterruptedException {
            Stats total = new Stats();
            for (Stats s : values) total.add(s);

            double num = total.n * total.sxy - total.sx * total.sy;
            double den = Math.sqrt((total.n * total.sxx - total.sx * total.sx)
                                 * (total.n * total.syy - total.sy * total.sy));
            double r = den == 0 ? 0.0 : num / den;
            ctx.write(key, new Text(String.valueOf(r)));
        }
    }

    @Override public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: hadoop jar tempavg-reduce.jar "
                    + "br.pucpr.mapreduce.CorrelationMain <entrada> <saida>");
            return -1;
        }
        Job job = Job.getInstance(getConf(), "Correlacao temp x RH");
        job.setJarByClass(CorrelationMain.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Stats.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CorrelationMain(), args));
    }
}
