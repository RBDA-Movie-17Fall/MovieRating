/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// $example on$

import java.util.*;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;

import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.DataFrame;
import scala.Serializable;
import scala.Tuple2;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

// $example off$

public class RandomForestRegression implements Serializable{

    public void randomForestModelTest(int numTrees) {
        // $example on$
        //run in self-contained mode
        SparkConf sparkConf = new SparkConf().setAppName("JavaRandomForestRegressionExample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Load and parse the data file.
        String datapath = "./input/imdb_5000.txt";

        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();

        // Split the data into training and test sets (20% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // Set parameters.
        // Empty categoricalFeaturesInfo indicates all features are continuous.
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();

        //int numTrees = 5000; // Use more in practice.

        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "variance";
        int maxDepth = 4;
        int maxBins = 32;
        int seed = 12345;
        // Train a RandomForest model.
        final RandomForestModel model = RandomForest.trainRegressor(trainingData,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);

        // Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        Double testMSE =
                predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Double> pl) {
                        Double diff = pl._1() - pl._2();
                        return diff * diff;
                    }
                }).reduce(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double a, Double b) {
                        return a + b;
                    }
                }) / testData.count();

        System.out.println("Test Mean Squared Error: " + testMSE);

        //System.out.println("Learned regression forest model:\n" + model.toDebugString());

        //get all the decision trees from the model
        DecisionTreeModel[] trees = model.trees();
        System.out.println("The first decision tree in the forest: " + trees[0].toDebugString());

        // Save and load
        model.save(jsc.sc(), "target/tmp/myRandomForestRegressionModel");
        RandomForestModel sameModel = RandomForestModel.load(jsc.sc(),
                "target/tmp/myRandomForestRegressionModel");
        // $example off$

        jsc.stop();
    }

    public void randomForestRegressionModel() {

        SparkConf conf = new SparkConf().setAppName("JavaRandomForestRegressorExample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // Load and parse the data file, converting it to a DataFrame.
        DataFrame data = sqlContext.read().format("libsvm").load("./input/imdb_5000.txt");

        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(data);

        // Split the data into training and test sets (30% held out for testing)
        DataFrame[] splits = data.randomSplit(new double[]{0.7, 0.3});
        DataFrame trainingData = splits[0];
        DataFrame testData = splits[1];

        // Train a RandomForest model.
        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("indexedFeatures");

        // Chain indexer and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{featureIndexer, rf});

        // Train model.  This also runs the indexer.
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        DataFrame predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("prediction", "label", "features").show(5);

        // Select (prediction, true label) and compute test error
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        double rmse = evaluator.evaluate(predictions);

        System.out.println();

        System.out.println("=============================RMSE===============================");

        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

        RandomForestRegressionModel rfModel = (RandomForestRegressionModel) (model.stages()[1]);

        //System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());

        //get the variable importance from the model
        double[] importances = rfModel.featureImportances().toArray();

        System.out.println();
        System.out.println("===================THE IMPORTANCE OF FEATURES===================");

        Map<Double, String> map = new TreeMap<>();
        map.put(importances[0], "Number of critic for reviews: ");
        map.put(importances[1], "Duration: ");
        map.put(importances[2], "Director Facebook likes: ");
        map.put(importances[3], "Actor 3 Facebook likes: ");
        map.put(importances[4], "Actor 1 Facebook likes: ");
        map.put(importances[5], "Revenue: ");
        map.put(importances[7], "Cast total Facebook likes: ");
        map.put(importances[8], "Face number in posters: ");
        map.put(importances[9], "Number of users for reviews: ");
        map.put(importances[10], "Budget: ");
        map.put(importances[11], "Year: ");
        map.put(importances[12], "Actor 2 Facebook likes: ");
        map.put(importances[13], "Aspect ratio: ");
        map.put(importances[14], "Movie Facebook likes: ");

        //print the feature importance in descending order
        List<Map.Entry<Double,String>> list = new ArrayList<>(map.entrySet());

        Collections.sort(list,new Comparator<Map.Entry<Double,String>>() {
            @Override
            public int compare(Map.Entry<Double, String> o1,
                               Map.Entry<Double, String> o2) {
                return o2.getKey().compareTo(o1.getKey());
            }

        });

        for(Map.Entry<Double, String> entry: list)
        {
            System.out.println(entry.getValue() + entry.getKey());
        }

        System.out.println();

        System.out.println("============================PREDICTION=========================");

        //predict the rating of "Justice League"
        double numCritics = 366; //Metascore critics
        double duration = 120;
        double directorLikes = 0;
        double act3Likes = 0;
        double act1Likes = 0;
        double revenue = 573000000;
        double numVoters = 129274;
        double castTotalLikes = 0;
        double faceNumPoster = 5;
        double numberReviewUsers = 1051;
        double budget = 300000000;
        double year = 2017;
        double act2Likes = 0;
        double aspectRatio = 1.85;
        double movieLikes = 0;

        int size = 15;
        int[] indices = new int[]{0,1,5,6,8,9,10,11,13};
        double[] values = new double[]{numCritics, duration, revenue, numVoters, faceNumPoster,
                                        numberReviewUsers, budget, year, aspectRatio};
        Vector justiceLeague = Vectors.sparse(size, indices, values);

        System.out.println("The predictive rating of \"Justice League\" is " + rfModel.predict(justiceLeague));

        System.out.println();

        System.out.println("================================================================");
    }

    public static void main(String[] args) {

        RandomForestRegression test = new RandomForestRegression();
        Scanner sc = new Scanner(System.in);

        while(true)
        {
            System.out.println("Show me what you got(1 - RMSE, 2 - Feature Importance & Justice League): ");
            System.out.print(">> ");
            int i = sc.nextInt();

            if(i == 1)
            {
                System.out.println("Number of trees in the random forest: ");
                System.out.print(">> ");
                int numTrees = sc.nextInt();
                test.randomForestModelTest(numTrees);
            }
            else if(i == 2)
            {
                test.randomForestRegressionModel();
            }
            else
                break;
        }

        sc.close();;

    }
}
