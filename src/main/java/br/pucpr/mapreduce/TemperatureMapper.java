package br.pucpr.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MAP
 *
 * Le o dataset Forest Fires (UCI), uma linha por registro, formato CSV:
 *
 *   X , Y , month , day , FFMC , DMC , DC , ISI , temp , RH , wind , rain , area
 *   0   1     2      3     4      5     6    7      8     9    10     11     12
 *
 * Ex.: 7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0
 *
 * Agrupamos a temperatura (coluna 8) pelo MES (coluna 2) e emitimos:
 *
 *      mes  ->  (temperatura, 1)
 *
 * A soma comeca sendo a propria temperatura e a contagem = 1.
 * NAO calculamos media aqui.
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {

    private static final int MONTH_INDEX = 2; // coluna "month"
    private static final int TEMP_INDEX = 8;  // coluna "temp"

    private final Text outKey = new Text();
    private final SumCountWritable outValue = new SumCountWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length <= TEMP_INDEX) {
            return; // linha sem colunas suficientes
        }

        String mes = fields[MONTH_INDEX].trim();
        String tempStr = fields[TEMP_INDEX].trim();

        double temperatura;
        try {
            temperatura = Double.parseDouble(tempStr);
        } catch (NumberFormatException e) {
            // Cabecalho (ex.: "temp") ou linha corrompida: conta e ignora.
            context.getCounter("temp", "linhas_invalidas").increment(1);
            return;
        }

        outKey.set(mes);
        outValue.set(temperatura, 1L); // (soma parcial = temperatura, contagem = 1)
        context.write(outKey, outValue);
    }
}
