package test.spark.arch;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.of;
import static test.spark.Utils.getResourceUrl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.SVMModel;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class App {

    public static void main(String[] args) {
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
//                    return new LabeledPoint(features[1], Vectors.dense(features[0], features[2]));
                }).randomSplit(new double[]{0.7, 0.3});

        JavaRDD<LabeledPoint> trainingData = splits[0].cache();
        JavaRDD<LabeledPoint> testData = splits[1].cache();

        System.out.println(trainingData.count());

        Vector vector = Vectors.dense(842,2,4,1);

        of(
            randomForestClassifier(trainingData, testData).predict(vector),
            randomForestRegressor(trainingData, testData).predict(vector),
            logisticRegression(trainingData).predict(vector),
            svm(trainingData).predict(vector),
            decisionTree(trainingData).predict(vector)
        ).forEach(s -> print().accept(s));
    }

    private static Consumer<Double> print() {
        return System.out::println;
    }

    private static DecisionTreeModel decisionTree(JavaRDD<LabeledPoint> trainingData) {
        int maxTreeDepth = 20;
        return DecisionTree.train(trainingData.rdd(), Algo.Classification(), Entropy.instance(), maxTreeDepth);
    }

    private static SVMModel svm(JavaRDD<LabeledPoint> trainingData) {
        int numIterations = 100;
        return SVMWithSGD.train(trainingData.rdd(), numIterations);
    }

    private static LogisticRegressionModel logisticRegression(JavaRDD<LabeledPoint> trainingData) {
        int numIterations = 100;
        return LogisticRegressionWithSGD.train(trainingData.rdd(), numIterations);
    }

    private static RandomForestModel randomForestClassifier(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 4;
        Integer maxBins = 32;
        Integer seed = 12345;

        RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

        JavaPairRDD<Double, Double> predictionAndLabel = testData
                .mapToPair((PairFunction<LabeledPoint, Double, Double>) p -> new Tuple2<>(model.predict(p.features()), p.label()));

        Double testErr = 1.0 * predictionAndLabel
                .filter((Function<Tuple2<Double, Double>, Boolean>) pl -> !pl._1().equals(pl._2()))
                .count() / testData.count();

        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification forest model:\n" + model.toDebugString());
        return model;
    }

    private static RandomForestModel randomForestRegressor(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 20;
        String featureSubsetStrategy = "auto";
        String impurity = "variance";
        Integer maxDepth = 10;
        Integer maxBins = 32;
        Integer seed = 12345;

        RandomForestModel model = RandomForest.trainRegressor(trainingData,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

        JavaPairRDD<Double, Double> predictionAndLabel = testData
                .mapToPair((PairFunction<LabeledPoint, Double, Double>) p -> new Tuple2<>(model.predict(p.features()), p.label()));

        Double testMSE = predictionAndLabel
                .map((Function<Tuple2<Double, Double>, Double>) pl -> {
                    Double diff = pl._1() - pl._2();
                    return diff * diff;
                }).reduce((Function2<Double, Double, Double>) (a, b) -> a + b) / testData.count();

        System.out.println("Test Mean Squared Error: " + testMSE);
        System.out.println("Learned regression forest model:\n" + model.toDebugString());
        return model;
    }
}
