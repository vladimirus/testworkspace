package test.spark.arch;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
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
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.impurity.Entropy;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

public class App {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<LabeledPoint>[] splits = sc.textFile(getResourceUrl("arch/Arch_Finance_parsed.txt"))
                .map(line -> line.split(","))
                .map(array -> {
                    List<Double> doubles = asList(array).stream()
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

        randomForestClassifier(trainingData, testData);
        randomForestRegressor(trainingData, testData);

        int numIterations = 100;
        int maxTreeDepth = 20;

        LogisticRegressionModel lrModel = LogisticRegressionWithSGD.train(trainingData.rdd(), numIterations);
        SVMModel svmModel = SVMWithSGD.train(trainingData.rdd(), numIterations);
        DecisionTreeModel treeModel = DecisionTree.train(trainingData.rdd(), Algo.Classification(), Entropy.instance(), maxTreeDepth);

        System.out.println(lrModel.predict(Vectors.dense(5.0,3.0,1)));
        System.out.println(svmModel.predict(Vectors.dense(5.0,3.0,1)));
        System.out.println(treeModel.predict(Vectors.dense(5.0,3.0,1)));
    }

    private static void randomForestClassifier(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
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
        System.out.println(model.predict(Vectors.dense(3.0,3.0,1.0))); //thur , 300 plus row, 1 day
    }

    private static void randomForestRegressor(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
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
        System.out.println(model.predict(Vectors.dense(5.0,381.0,1.0)));
    }
}
