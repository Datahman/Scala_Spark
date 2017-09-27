package pacman

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
import org.apache.spark.rdd.RDD
import org.dianahep.histogrammar._
import org.dianahep.histogrammar.ascii._
import com.quantifind.charts.Highcharts._

object CheckinsTestv2 {

  // Define case class for training and test data sets
  case class FBtrain(CHECKINID:Double,xLOC: Double, yLOC: Double, accuracy: Double, time: Double, PLACE_ID: String)
  case class FBtest(CHECKINID:Double,xLOC: Double, yLOC: Double, accuracy: Double, time: Double)
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("FBCheckins")
      conf.setMaster("local[2]")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      import sqlContext.implicits._

      //val inData = sc.textFile("hdfs:///user/cloudera/Desktop/trainsample.csv").map(_.split(" "))

      val intrainData = sc.textFile("trainsample.csv").map(_.split(" "))
     // val intestData = sc.textFile("test.csv").map(_.split(" "))

      val headertrain = intrainData.first
    //  val headertest = intestData.first
      val falseheadertrainData = intrainData.filter(_ (0) != headertrain(0)) // Remove header
  //    val falseheadertestData = intestData.filter(_(0) != headertest(0))

      // Change RDDs to DF
      val trainData = falseheadertrainData.map(_ (0))
        .map(x => x.split(","))
        .map(x => FBtrain( x(0).toDouble,
          x(1).toDouble,
          x(2).toDouble,
          x(3).toDouble,
          x(4).toDouble,
          x(5)
        ))
        .toDF("CHECKINID","XLOC", "YLOC", "ACCURACY", "TIMESTAMP", "PLACE_ID")
/*
      val testData = falseheadertestData.map(_ (0))
        .map(x => x.split(","))
        .map(x => FBtest( x(0).toDouble,
          x(1).toDouble,
          x(2).toDouble,
          x(3).toDouble,
          x(4).toDouble
        ))
        .toDF("CHECKINID","XLOC", "YLOC", "ACCURACY", "TIMESTAMP")
*/
      // Make test data set based on similar x- Locations.

      //val GroupNumber = trainData.select("PLACE_ID").distinct().count()
       println("Number of unique class labels are: "+trainData.select("PLACE_ID").distinct().count() ) // output: 108390

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



      /* Model Tuning */

      val paramGrid = new ParamGridBuilder()
        .addGrid(RF.numTrees, Array(1))
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
        .setNumFolds(2) // Set K- fold





      println("Testing \n")

      val cvModel = cv.fit(trainData)
      println("Printing predictions Schema \n")
      val modelDF = cvModel.transform(trainData).toDF().select("PLACE_ID", "PredictedPLACE_ID")

    /*
      val run = trainData.map {
        case LabeledPoint(label, features) =>
          val prediction = modelDF.
          (prediction, label)
      }
*/
    /* val EstimatedTestingData = cvModel.transform(trainData)


      EstimatedTestingData.printSchema()


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
----------------------------------------------
*/

        /*
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
     */

    /*
      val FalseLabelCount = DemoResults.filter(tup => ( tup._1 != tup._2) ).count()

      val SimilarLabels = DemoResults.filter(tup => ( tup._1 == tup._2) ).count()

      println("False count: " + FalseLabelCount)
      println(" Similar label count: " +SimilarLabels)
      val TotalLabelCount = DemoResults.map(tup=> tup).count()
      println("Total label count: " +TotalLabelCount)

      // Test error ratio
      println("Test error ratio: " +(FalseLabelCount / TotalLabelCount).toFloat )

      // Correct classification percentage
      val precision = ((TotalLabelCount - FalseLabelCount) / TotalLabelCount).toFloat
      println("Precision: " + (precision * 100) )



    trainData.registerTempTable("traintable")
---------------------
*/






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













    //sqlContext.sql(" SELECT  PLACE_ID , COUNT(PLACE_ID) as CLASS_LABEL_COUNT FROM traintable GROUP BY PLACE_ID   ").show(10)

    // Find frequency of class labels

   /* val labelFreqCount = sqlContext.sql(" SELECT  PLACE_ID , COUNT(PLACE_ID) as CLASS_LABEL_COUNT FROM traintable GROUP BY PLACE_ID   ")
      .toDF()
      .select("PLACE_ID", "CLASS_LABEL_COUNT").map({case
                Row(
        place_id: String,
        class_label_count: Long
        ) => (
      place_id.toDouble,
      class_label_count.toInt
    )
    }).map(tup => tup._2).collect()
      val Seqlabel: Seq[Int] = labelFreqCount.toSeq
*/
    //println(Seqlabel.length)
      //println(Seqlabel)


    /*
      histogram(Seqlabel,83451) // Use DISTINCT(CLASS_LABEL_COUNT) FOR BIN #
    xAxis("Class labels")
    yAxis("Count per label")

*/
      //labelFreqArray.map(tup => (tup._1,tup._2 ) ).foreach(println)
/*
    labelFreqCount.

      val demohisto = Bin(5,0,10, {count: Int => count })
      for(count <- 1 until labelFreqCount.length) {
          demohisto.fill(labelFreqCount(count))
      }
      demohisto.println
      //println(demohisto.values)


*/





    // Find max count of class label on the train set

   // sqlContext.sql(" SELECT   MAX(CLASS_LABEL_COUNT) as MAX_CLASS_COUNT FROM (SELECT  PLACE_ID , COUNT(PLACE_ID) as CLASS_LABEL_COUNT FROM traintable GROUP BY PLACE_ID ) as t  ")












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
