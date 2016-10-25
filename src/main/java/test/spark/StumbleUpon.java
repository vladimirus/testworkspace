package test.spark;

import static java.util.Arrays.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.LabeledPoint;

public class StumbleUpon {

    public static void main(String[] args) {
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("SparkTest")
//                .master("local[*]")
//                .getOrCreate();

//        Dataset<Row> dataset = spark.read()
//                .option("header", true)
//                .option("delimiter", "\t")
//                .csv("/home/vov/IdeaProjects/ObserverTest/src/main/resources/train.tsv");

//        dataset.select("label").show();
//        dataset.show();
//        spark.stop();

        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String[]> data = sc.textFile("/home/vov/IdeaProjects/ObserverTest/src/main/resources/train_noheader.tsv")
                .map(line -> line.split("\t"))
                .map(array -> stream(array).map(item -> item.replaceAll("\"", "")).toArray((String[]::new)));


        JavaRDD<LabeledPoint> labelPoints = data.map(array -> {
            //something

            double[] features;
//            return new LabeledPoint(1.0, Vectors.dense(features));
            return null;
        });


        System.out.println(data.first()[0]);




//        dataset.map( r ->
//            val trimmed = r.map(_.replaceAll("\"", ""))
//            val label = trimmed(r.size - 1).toInt
//            val features = trimmed.slice(4, r.size - 1).map(d => if (d ==
//                    "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)
//            LabeledPoint(label, Vectors.dense(features))
//        }


    }

}
