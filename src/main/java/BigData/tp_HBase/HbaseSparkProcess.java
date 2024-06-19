package BigData.tp_HBase;//package BigData.tp_HBase;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//public class HbaseSparkProcess {
//    public void createHbaseTable() {
//        Configuration config = HBaseConfiguration.create();
//        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[4]");
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        config.set(TableInputFormat.INPUT_TABLE,"products");
//        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
//                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//        System.out.println("nombre d'enregistrements: "+hBaseRDD.count());
//    }
//    public static void main(String[] args){
//        HbaseSparkProcess admin = new HbaseSparkProcess();
//        admin.createHbaseTable();
//    }
//}
//
//package BigData.tp_HBase;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import scala.Tuple2;
//
//public class HbaseSparkProcess {
//    public void createHbaseTable() {
//        Configuration config = HBaseConfiguration.create();
//        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[4]");
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        config.set(TableInputFormat.INPUT_TABLE, "products");
//        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
//                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//
//        // Transformation pour extraire les valeurs de la colonne 'cf:sales' et les convertir en nombres
//        JavaRDD<Integer> salesRDD = hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Integer>() {
//            @Override
//            public Integer call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
//                Result result = tuple._2;
//                byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("sales"));
//                String valueStr = Bytes.toString(valueBytes);
//                return Integer.parseInt(valueStr);
//            }
//        });
//
//        // Faire la somme des ventes
//        int totalSales = salesRDD.reduce(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer a, Integer b) {
//                return a + b;
//            }
//        });
//
//        System.out.println("Total des ventes: " + totalSales);
//    }
//
//    public static void main(String[] args) {
//        HbaseSparkProcess admin = new HbaseSparkProcess();
//        admin.createHbaseTable();
//    }
//}


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HbaseSparkProcess {

    public void calculateTotalSales() {
        // Configuration HBase
        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, "products"); // Remplacer "products" par le nom de votre table HBase

        // Configuration Spark
        SparkConf sparkConf = new SparkConf().setAppName("HBaseSparkProcess").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Charger les données de HBase dans un RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // Extraire les prix des transactions et les convertir en double
        JavaRDD<Double> pricesRDD = hBaseRDD.map(tuple -> {
            Result result = tuple._2();
            byte[] priceBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"));
            if (priceBytes != null) {
                try {
                    return Double.parseDouble(Bytes.toString(priceBytes));
                } catch (NumberFormatException e) {
                    // En cas de problème de conversion, retourner 0
                    return 0.0;
                }
            }
            return 0.0;
        });

        // Calculer la somme totale des prix
        double totalSum = pricesRDD.reduce((a, b) -> a + b);

        // Afficher le total des ventes
        System.out.println("Total des ventes de tous les produits: $" + totalSum);

        // Fermer le contexte Spark
        jsc.close();
    }

    public static void main(String[] args) {
        HbaseSparkProcess processor = new HbaseSparkProcess();
        processor.calculateTotalSales();
    }
}
