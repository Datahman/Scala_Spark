package pacman
/*
Altered version where two metric results are shown for TWO  models of distinct K- number of trees
*/

/**
  * Created by path2 on 23/05/17.
  * Goals: To construct a machine learning pipeline in spark,
  * and to give out the various metrics for the classification of the test data set against a model. 
  * 
  */

import breeze.linalg.DenseVector
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
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

object CheckinsTest {

  // Define case class for training and test data sets
  case class FBtrain(CHECKINID: Double, xLOC: Double, yLOC: Double, accuracy: Double, time: Double, PLACE_ID: String)

  case class FBtest(CHECKINID: Double, xLOC: Double, yLOC: Double, accuracy: Double, time: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("FBCheckins")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    // READ IN TRAINING AND TEST DATA FROM FILE

    val intrainData = sc.textFile("trainsampledummy.csv").map(_.split(" "))
    // val intestData = sc.textFile("test.csv").map(_.split(" "))

    val headertrain = intrainData.first
    //  val headertest = intestData.first
    val falseheadertrainData = intrainData.filter(_ (0) != headertrain(0)) // Remove header
    //    val falseheadertestData = intestData.filter(_(0) != headertest(0))

    // Change RDDs to DF
    val trainData = falseheadertrainData.map(_ (0))
      .map(x => x.split(","))
      .map(x => FBtrain(x(0).toDouble,
        x(1).toDouble,
        x(2).toDouble,
        x(3).toDouble,
        x(4).toDouble,
        x(5)
      ))
      .toDF("CHECKINID", "XLOC", "YLOC", "ACCURACY", "TIMESTAMP", "PLACE_ID")
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
    println("Number of unique class labels are: " + trainData.select("PLACE_ID").distinct().count()) // output: 108390

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
      .setOutputCol("features")


    /*
    * BUILD A MACHINE LEARNING PIPELINE
    */

    // val Array(trainingData, testData) = DF.randomSplit(Array(0.7, 0.3))

    val ArrayIndices = LabelIndexer.fit(trainData)

    val RF = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")


    // Revert back indexed labels to original labels i.e PLACE_ID FORMAT
    val LabelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("PredictedPLACE_ID")
      .setLabels(ArrayIndices.labels)


    val pipeline = new Pipeline()
      .setStages(Array(LabelIndexer, Assembler, RF, LabelConverter))


    /* Model Tuning */

    val paramGridModel1 = new ParamGridBuilder()
      .addGrid(RF.numTrees, Array(10))
      .addGrid(RF.minInfoGain, Array(0.0))
      .addGrid(RF.featureSubsetStrategy, Array("sqrt"))
      .addGrid(RF.maxBins, Array(50))
      .addGrid(RF.seed, Array(12345L))
      .build()

    val paramGridModel2 = new ParamGridBuilder()
      .addGrid(RF.numTrees, Array(5))
      .addGrid(RF.minInfoGain, Array(0.0))
      .addGrid(RF.featureSubsetStrategy, Array("sqrt"))
      .addGrid(RF.maxBins, Array(50))
      .addGrid(RF.seed, Array(12345L))
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
      .setEstimatorParamMaps(paramGridModel1)
      .setNumFolds(2) // Set K- fold


    println("Testing \n")


    val Model1 = cv.fit(trainData)
    println("Printing predictions Schema \n")
    val mod1 = Model1.transform(trainData).toDF()

    val PredLabelModel1 = mod1.select("CHECKINID", "features", "label","prediction", "PredictedPLACE_ID", "PLACE_ID").map({
      case Row(id: Double,
      features: Vector,
      labels: Double,
      prediction: String,
      place_id: String) => (
        (id.toDouble, features.toDense, labels.toDouble, prediction, place_id.toString)

        )
    })
    PredLabelModel1.foreach(println)


    val cv2 = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator()
        .setPredictionCol("prediction") // RF gives prediction column
        .setMetricName("precision")
        .setLabelCol("label")) // label is the indexed PLACE_ID!
      .setEstimatorParamMaps(paramGridModel2)
      .setNumFolds(4) // Set K- fold


    println("Testing \n")


    val Model2 = cv2.fit(trainData)


    val mod2 = Model2.transform(trainData).toDF()
    mod2.printSchema()
    println("Printing predictions Schema \n")
    val PredLabelModel2 = mod2.select("CHECKINID", "features", "label", "prediction","PredictedPLACE_ID", "PLACE_ID").map({
      case Row(id: Double,
      features: Vector,
      labels: Double,
      prediction: String,
      place_id: String) => (
        (id.toDouble, features.toDense, labels.toDouble, prediction, place_id.toString)

        )
    })


    //val scoreAndLabels: RDD[(Array[Double], Array[Double])]
    // DenseVector,Double,String,String


    var StorageArray = PredLabelModel1
    StorageArray ++= PredLabelModel2
    StorageArray.foreach(println)

    //println(StorageArray.collect().length) //20




    StorageArray.toDF()

    val PREDICTIONCLASS_ACTUALCLASS_ARRAY = StorageArray.map(row => (row._3,row._4))
    val PC_ACLabel = PREDICTIONCLASS_ACTUALCLASS_ARRAY
      .map(row => (row._1,row._2.toDouble) )
      .map { case ((predicted_class_index: Double, actual_class_index : Double) )
      => (predicted_class_index, actual_class_index)
      }.collect().foreach(println)
    // TO DO MAKE ARRAY [PREDICTED CLASS, ACTUAL CLASS ] FOR EACH ROW
    // ALSO TO DO: ADD PREDICTION INDEX FOR ABOVE 


    // Match different model predicted class labels based on UNIQUE ID.
    val ID_PREDICTIONCLASS_FREQUENCY_Array = StorageArray.map(row => (row._1, row._2.toArray, row._3, row._4, row._5))
    val IdPredictedClassLabel = ID_PREDICTIONCLASS_FREQUENCY_Array.map(row => (row._1, row._3, 1))
      .map { case ((id: Double, predictionlabel: Double, value: Int))
      => ((id, predictionlabel), value)}
      .reduceByKey((val1, val2) => val1 + val2) // vals are current, next value pair
      .collect()
      .foreach(println)



  }
}



    /*

    val modelApredictionAndLabels = trainData.rdd.map { case LabeledPoint(label, features) =>
      val prediction = cv.fit(trainData)//getEvaluator.evaluate(trainData) // .getEvaluator.evaluate(trainData)

      (prediction, label)

    }
   val  modelApredictionAndLabels_METRICS = new MulticlassMetrics(modelApredictionAndLabels)

    // Precision by label
    val labels = modelApredictionAndLabels_METRICS.labels
    labels.foreach( l =>
      println(s"Precision($l) = " + modelApredictionAndLabels_METRICS.precision(l))
    )

*/

