package com.packagename

/**
  * Created by cloudera on 5/4/17.
  * Demonstraion of stateful transformations on streaming RDD batches. 
  * 
  */

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.spark.streaming.{Seconds, StreamingContext}
object DStreams {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create Spark context for the app
    val sparkConf = new SparkConf().setAppName("StreamingErrorCount").setMaster("local[2]")


    val BatchInterval = 2 // No. of RDD in each stream.

    val WindowInterval = 10 // Similar to the BatchInterval requires SlideInterval

    val SlideInterval = 2 // WindowInterval fraction time to produce RDD. So 10/2 == 5 RDDs.
    
    // Define a Spark streaming context to take in streams at Batch second interval
    val ssc = new StreamingContext(sparkConf,Seconds(BatchInterval)) 

    ssc.checkpoint("file:///tmp/spark")

    val Localhost_Port_Array: Array[String] = new Array[String](2)

    Localhost_Port_Array(0) = "localhost"
    Localhost_Port_Array(1) = "9999"

    val Hostname = Localhost_Port_Array(0)

    val Portnumber = Localhost_Port_Array(1).toInt

    
    // Read data
    val lines = ssc.socketTextStream(Hostname, Portnumber)

    // Stateless counts variable
     /* val counts = lines.flatMap(_.split(" "))
                .filter(_.contains("ERROR"))
                .map((_,1)).map(_.swap)
                .reduceByKey(_+_)

    */

    //println("Original STREAM")

    //println("Transformed STREAM")


    // counts.print()

    /*
      Stateful transformations: Applicable to streaming data (Multiple RDDs).

      * updateStateByKey(): Extension of reduceByKey method. Treats all RDDs within a stream to same Key: K

    */

    /* Aggregate multiple batch stream count
       Version 1
       WORKS (?)
       Short version
      */

     val updatestate = (values: Seq[Int], state: Option[Int] ) => {
      val currentCount = values.foldLeft(0)(_+_) // Start iteration at 0, and from left. Changes Seq[Int] => T[Int]
      val previousCount = state.getOrElse(0) // Return previous int when T[Int] present , or else 0 (case first batch).
      Some(currentCount + previousCount) // return T[Int] which is equivalent to state: Option[Int]
    }

    val wordDStream = lines.flatMap(_.split(" "))
                      .filter(_.contains("ERROR"))
                      .map((_,1))//.reduceByKeyAndWindow((a:Int,b:Int) => (a+b),Seconds(10),Seconds(2))

   


    // WORKS- wordDStream.print()
    val aggregatewordstream = wordDStream.updateStateByKey(updatestate)
    //println("Aggregates")

    // WORKS - aggregatewordstream.print()



    /* Aggregate multiple batch stream count
       Version 2
       WORKS (?)
       Longer version: Commented out
      */

    /*

    def updatestate(newValues: Seq[(Int)], currentValue: Option[(Int)]): Option[(Int)] =
    {

       var result: Option[(Int)] = Option(0) // Set initial to 0

      if(newValues.isEmpty) { // If newValues not provided, set currentValue as the result
        result =  Some(currentValue.get)
      }
      else {
        newValues.foreach{x => {
          if (currentValue.isEmpty) {
            result = Some(x)
          }
          else {
            // If previous value (current value) present, add to result
            result = Some(x + result.get)
          }
        }
        }
      }
      result
    }
    /*




    /* TO  FIX

    def updateValues(newValues: Seq[(Int, String)], currentValue: Option[(Int, String)]): Option[(Int, String)] =
    {
      var result: Option[(Int, String)] = null // Set initial return value to null

      if(newValues.isEmpty) { // If key isn't in batch return previous count result
        result = Some(currentValue.get)
      }
      else {
        newValues.foreach{x => {
          if (currentValue.isEmpty) {
            result = Some(x)
          }
          else {
            result = Some( (currentValue.get._1 + x._1),currentValue.get._2)
          }
          }
        }
      }
    result
    }


    // Count number of lines within the Windowed interval
    val WindowedDStream = lines.countByWindow(Seconds(WindowInterval),Seconds(SlideInterval))
    // WORKS- WindowedDStream.print()

    // Count number of lines within the Windowed interval
    val ReducedWindowedDStream = lines.reduceByWindow(_+_,Seconds(WindowInterval),Seconds(SlideInterval) )
    // WORKS- ReducedWindowedDStream.print()

    ssc.start()
    ssc.awaitTermination()


  }





}





*/

  /* Relevant links:
  * http://amithora.com/spark-update-by-key-explained/
  * http://stackoverflow.com/questions/24771823/spark-streaming-accumulated-word-count
  * http://stackoverflow.com/questions/27535668/spark-streaming-groupbykey-and-updatestatebykey-implementation
 */