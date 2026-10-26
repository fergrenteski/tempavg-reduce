package br.pucpr.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * MAIN / DRIVER
 *
 * Configura e dispara o job MapReduce que calcula a media de temperaturas.
 *
 * Uso:
 *      hadoop jar tempavg-reduce.jar <entrada> <saida>
 */
public class Main extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: hadoop jar tempavg-reduce.jar <entrada> <saida>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Media de Temperaturas");
        job.setJarByClass(Main.class);

        // Map
        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCountWritable.class);

        // Combiner (OPCIONAL, mas seguro):
        // como o valor carrega (soma, contagem), agregar parcialmente em cada no
        // produz exatamente o mesmo resultado que agregar tudo no reduce final.
        // Se emitissemos medias, ligar o combiner daria resultado ERRADO.
        job.setCombinerClass(SumCountCombiner.class);

        // Reduce
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(exitCode);
    }

    /**
     * Combiner: apenas SOMA as somas e SOMA as contagens (nao divide).
     * Mantem o tipo de saida igual ao do Map (Text, SumCountWritable),
     * por isso pode rodar entre o Map e o Reduce sem alterar o resultado.
     */
    public static class SumCountCombiner
            extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {

        private final SumCountWritable parcial = new SumCountWritable();

        @Override
        protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
                throws IOException, InterruptedException {

            double soma = 0.0;
            long contagem = 0L;
            for (SumCountWritable v : values) {
                soma += v.getSum();
                contagem += v.getCount();
            }
            parcial.set(soma, contagem); // repassa o par agregado, SEM dividir
            context.write(key, parcial);
        }
    }
}
