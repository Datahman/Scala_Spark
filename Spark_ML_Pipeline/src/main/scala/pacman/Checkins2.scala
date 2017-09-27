package pacman
/*
* Goals: To construct a machine learning pipeline in spark,
* and to give out the various metrics for the classification of the test data set against a model. 
*/

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.mllib.evaluation.MulticlassMetrics


object Checkins2 {

  // Define case class for our data
  case class FB(CHECKINID:Double,xLOC: Double, yLOC: Double, accuracy: Double, time: Double, PLACE_ID: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("FBCheckins")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    //val inData = sc.textFile("hdfs:///user/cloudera/Desktop/trainsample.csv").map(_.split(" "))

    val inData = sc.textFile("trainsample.csv").map(_.split(" "))

    val header = inData.first

    val falseheaderData = inData.filter(_ (0) != header(0)) // Remove header

    // Change RDD to DF
    val trainData = falseheaderData.map(_ (0))
      .map(x => x.split(","))
      .map(x => FB( x(0).toDouble,
        x(1).toDouble,
        x(2).toDouble,
        x(3).toDouble,
        x(4).toDouble,
        x(5)
      ))
      .toDF("CHECKINID","XLOC", "YLOC", "ACCURACY", "TIMESTAMP", "PLACE_ID")




    /*
  * Feature transformers.
  *
  * StringIndexer: Uses string as an input, throws out numbers based on input string frequency
  * e.g: COLUMN: A , A , B , A , B, C (StringIndexer) => (A,B,C) => (0,1,2)
  */

    // Transform the classifier labels with respective indices

    val LabelIndexer = new StringIndexer()
      .setInputCol("PLACE_ID")
      //.setOutputCol("PLACE_ID_INDEXED")
      .setOutputCol("label")
      .setHandleInvalid("skip") // Skip on labels that mis-match between test/training data-set


    val NumericFeatsArray = Array("XLOC", "YLOC", "ACCURACY", "TIMESTAMP")


    val Assembler = new VectorAssembler()
      .setInputCols(NumericFeatsArray)
      .setOutputCol("Features")


    /*
    * BUILD A MACHINE LEARNING PIPELINE
    */

   // val Array(trainingData, testData) = DF.randomSplit(Array(0.7, 0.3))

    val ArrayIndices = LabelIndexer.fit(trainData)

    val RF = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("Features")


    // Revert back indexed labels to original labels i.e PLACE_ID FORMAT
    val LabelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("PredictedPLACE_ID")
      .setLabels(ArrayIndices.labels)


    val pipeline = new Pipeline()
      .setStages(Array(LabelIndexer, Assembler, RF, LabelConverter))

/*
    // Fit the pipeline to training data
    val model = pipeline.fit(trainingData) // Estimator.


    val predictions = model.transform(testData)


    predictions.printSchema()
    println("Testing \n")
    predictions.select("PredictedPLACE_ID", "label", "Features").show(10, false)



    val predictionsa = predictions
      .select(predictions("label"),
      predictions("prediction").cast(DoubleType).as("prediction"))

    predictionsa.printSchema()

    val PredictionsAndLabels = predictionsa.select("prediction", "label")


    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precision") // accuracy not supported in Spark 1.6

    val precisions = evaluator.evaluate(PredictionsAndLabels)
        println("Precision ratio = " + precisions)

*/


    /* Model Tuning */

    val paramGrid = new ParamGridBuilder()
      .addGrid(RF.numTrees, Array(10))
      .addGrid(RF.minInfoGain,Array(0.0))
      .addGrid(RF.featureSubsetStrategy,Array("sqrt"))
      .addGrid(RF.maxBins,Array(50))
      .addGrid(RF.seed,Array(12345L))
      .build()

    /* Treat the Pipeline as an estimator , wrapping around the Crossvalidator instance,
    allowig us to jointly set parameters for the entire pipeline.
    */
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator()
        .setPredictionCol("prediction") // RF gives prediction column
        .setMetricName("precision")
        .setLabelCol("label")) // label is the indexed PLACE_ID!
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(5) // Set K- fold

    /*
    ({
      rfc_dcf410b7b938-featureSubsetStrategy: sqrt,
      rfc_dcf410b7b938-maxBins: 50,
      rfc_dcf410b7b938-maxDepth: 4,
      rfc_dcf410b7b938-minInfoGain: 0.0,
      rfc_dcf410b7b938-numTrees: 15,
      rfc_dcf410b7b938-seed: 12345
    },0.007412460953555855)
    ({*/



    println("Testing \n")

    val cvModel = cv.fit(trainData)
    println("Printing predictions Schema \n")
    val EstimatedTestingData = cvModel.transform(trainData)
    EstimatedTestingData.toDF().show(100)
    EstimatedTestingData.printSchema()
    var count = 0

    // Store actual and predicted labels onto a tuple.
    val DemoResults = EstimatedTestingData.toDF()
    .select("PLACE_ID","PredictedPLACE_ID")
    .map({
      case Row (
      place_id: String,
      predicted_placeid: String) => (
        (place_id.toString,predicted_placeid.toString)
        )
    })

    val FalseLabelCount = DemoResults.filter(tup => ( tup._1 != tup._2) ).count()

    val SimilarLabels = DemoResults.filter(tup => ( tup._1 == tup._2) ).count()

    println(FalseLabelCount)
    println(SimilarLabels)
    val TotalLabelCount = DemoResults.map(tup=> tup).count()
    println(TotalLabelCount)
    // Correc classification percentage
    print(((TotalLabelCount - FalseLabelCount) / TotalLabelCount).toFloat * 100.0)

    /*






    val TestingResults = EstimatedTestingData.toDF()
      .select("prediction","label")
      .map({case
        Row(
        prediction: Double,
        label: Double)
        =>
        (
          prediction.toDouble,
          label.toDouble)})




    // Feed testing result tuple (prediction,label) to MultiClassifier estimator
    val TestingResultsLabels = TestingResults.map(xrowTuple => (xrowTuple._1,xrowTuple._2))
    // Get evaluation metrics
    val testingMulticlassMetrics = new MulticlassMetrics(TestingResultsLabels)
    // Get metric values
     val testingPrecision = testingMulticlassMetrics.precision
     val testingConfusion = testingMulticlassMetrics.confusionMatrix




    // Print results
    println("Testing result precision: " +testingPrecision.toString)
    //println("Testing result Confusion Matrix " +testingConfusion )







    val Metrics = cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)
    Metrics.foreach(println)

*/




  }
}
