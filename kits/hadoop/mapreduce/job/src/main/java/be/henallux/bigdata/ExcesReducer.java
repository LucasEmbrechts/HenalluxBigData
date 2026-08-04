package be.henallux.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExcesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable resultat = new IntWritable();

    @Override
    protected void reduce(Text camion, Iterable<IntWritable> valeurs, Context context)
            throws IOException, InterruptedException {

        int total = 0;
        for (IntWritable v : valeurs) {
            total += v.get();
        }

        resultat.set(total);
        context.write(camion, resultat);
    }
}
