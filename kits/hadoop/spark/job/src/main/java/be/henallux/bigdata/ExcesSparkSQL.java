package be.henallux.bigdata;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ExcesSparkSQL {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Usage : ExcesSparkSQL <entree_hdfs> <sortie_hdfs>");
            System.exit(-1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Exces de vitesse - DataFrame")
                .getOrCreate();

        Dataset<Row> releves = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(args[0]);

        releves.printSchema();

        Dataset<Row> resultat = releves
                .filter(col("vitesse").gt(90))
                .groupBy("camion_id")
                .count()
                .orderBy(col("count").desc());

        resultat.show(50);   // ACTION

        releves.createOrReplaceTempView("releves");
        spark.sql(
                "SELECT camion_id, COUNT(*) AS nb_exces " +
                "FROM releves WHERE vitesse > 90 " +
                "GROUP BY camion_id ORDER BY nb_exces DESC")
             .show(50);

        releves.groupBy("camion_id")
               .agg(avg("vitesse").alias("vitesse_moyenne"))
               .orderBy(col("vitesse_moyenne").desc())
               .show(50);

        // Ecriture du resultat dans HDFS
        resultat.write().mode("overwrite").option("header", true).csv(args[1]);

        spark.stop();
    }
}
