package be.henallux.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class ExcesSparkRDD {

    private static final int LIMITE = 90;

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Usage : ExcesSparkRDD <entree_hdfs> <sortie_hdfs>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().setAppName("Exces de vitesse - RDD");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lignes = sc.textFile(args[0]);

        JavaPairRDD<String, Integer> resultat = lignes
                .filter(l -> !l.startsWith("camion_id") && !l.trim().isEmpty())
                .map(l -> l.split(","))
                .filter(l -> estExces(l))
                .mapToPair(champs -> new Tuple2<>(champs[0], 1))
                .reduceByKey((a, b) -> a + b);

        resultat.saveAsTextFile(args[1]);

        sc.close();
    }

    public boolean estExces(String[] champs) {
        if (champs.length < 2) {
            return false;
        }
        try {
            return Double.parseDouble(champs[1]) > LIMITE;
        } catch (NumberFormatException e) {
            return false;   // ligne malformee : ignoree
        }
    }
}
