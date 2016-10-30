package test.spark.arch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.impurity.Entropy;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static test.spark.Utils.getResourceUrl;

public class App {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<LabeledPoint>[] splits = sc.textFile(getResourceUrl("arch/Arch_Finance_parsed.txt"))
                .map(line -> line.split(","))
                .map(array -> {
                    List<Double> doubles = stream(array)
                            .map(Double::valueOf)
                            .collect(toList());

                    Double label = doubles.remove(doubles.size() - 1);
                    double[] features = doubles.stream().mapToDouble(d -> d).toArray();
                    return new LabeledPoint(label, Vectors.dense(features));
                }).randomSplit(new double[]{0.7, 0.3});

        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1].cache();

        Vector vector = Vectors.dense(835,2,1,4.19);

        Collection<Tuple2<Double, String>> list = Stream.<Function<Vector, Double>>of(
                randomForestRegressor(training)::predict,
                randomForestClassifier(training)::predict,
                logisticRegression(training)::predict,
                svm(training)::predict,
                decisionTree(training)::predict
        ).map(v -> {
            try {
                return new Tuple2<>(v.call(vector), accuracy(test, v));
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }).collect(toList());

        sc.stop();

        list.forEach(print());

    }

    private static <T> Consumer<T> print() {
        return System.out::println;
    }

    private static DecisionTreeModel decisionTree(JavaRDD<LabeledPoint> data) {
        int maxTreeDepth = 30;
        return DecisionTree.train(data.rdd(), Algo.Classification(), Entropy.instance(), maxTreeDepth);
    }

    private static ClassificationModel svm(JavaRDD<LabeledPoint> data) {
        int numIterations = 100;
        return SVMWithSGD.train(data.rdd(), numIterations);
    }

    private static ClassificationModel logisticRegression(JavaRDD<LabeledPoint> data) {
        int numIterations = 100;
        return LogisticRegressionWithSGD.train(data.rdd(), numIterations);
    }

    private static RandomForestModel randomForestClassifier(JavaRDD<LabeledPoint> data) {
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 4;
        Integer maxBins = 32;
        Integer seed = 12345;

        return RandomForest.trainClassifier(data, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
    }

    private static RandomForestModel randomForestRegressor(JavaRDD<LabeledPoint> data) {
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 20;
        String featureSubsetStrategy = "auto";
        String impurity = "variance";
        Integer maxDepth = 10;
        Integer maxBins = 32;
        Integer seed = 12345;

        return RandomForest.trainRegressor(data, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
    }

    private static String accuracy(JavaRDD<LabeledPoint> data, Function<Vector, Double> predict) {
        return format("%.2f%%", (double)(data
                .map(point -> predict.call(point.features()) == point.label() ? 1 : 0)
                .reduce((a, b) -> a + b)) / data.count() * 100);
    }
}
