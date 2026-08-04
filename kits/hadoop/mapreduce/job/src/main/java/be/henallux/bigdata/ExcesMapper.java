package be.henallux.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExcesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int LIMITE = 90;

    private final Text camion = new Text();
    private final IntWritable un = new IntWritable(1);

    @Override
    protected void map(LongWritable cle, Text valeur, Context context)
            throws IOException, InterruptedException {

        String ligne = valeur.toString().trim();

        if (ligne.isEmpty() || ligne.startsWith("camion_id")) {
            return;
        }

        String[] champs = ligne.split(",");
        if (champs.length < 2) {
            return;
        }

        try {
            double vitesse = Double.parseDouble(champs[1]);

            if (vitesse > LIMITE) {
                camion.set(champs[0]);
                context.write(camion, un);
            }
        } catch (NumberFormatException e) {
        }
    }
}
