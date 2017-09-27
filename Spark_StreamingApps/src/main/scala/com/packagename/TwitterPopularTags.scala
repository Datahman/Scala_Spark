/**
Demonstration of Spark Streaming using a twitter API.
  The script counts the frequency of popular hash tags in the last 60 seconds.

  * Arguments:
  * <consumerkey>
  * <consumerSecret>
  * <access token>
  * <access token Secret>
  * [<filters>] : A single word or list of words referencing to the twitter account

  */
package com.packagename

// Import the necessary library

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
/*import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.dstream.DStream
*/

object TwitterPopularTags {
  def main(args: Array[String]) {
    println("Hello")
    println(args.length)

    // Sanity check. Ensure four arguements are provided.
    
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " + "<access token> <access token secret> [<filters>]")
      System.exit(1)

    }


    // Declaration and storage of Twitter API parameters to an array
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    // Add filter to the end of the array i.e at position (0) from Right
    val filters = args.takeRight(args.length - 4)

    /* Set System properties to validate OAuth between
    twitter library and the app
    */
    
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Spark context for the app
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[2]")
    // Define a Spark streaming context to take in streams at two second interval
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    // Create a twitter stream
    val stream = TwitterUtils.createStream(ssc, None, filters)
    // Extract only the hash tags from stream input
    val hashTags = stream.flatMap( lines => lines.getText.split(" ")).filter(_.startsWith("#"))

    // Save batch streams
    hashTags.saveAsTextFiles("file:///home/cloudera/IdeaProjects/TwitterSparkStreaming/STREAMS","txt")

    // Top hash tags in the last 60 seconds
    val Tophashtags60 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_,Seconds(60))
      .map(_.swap) // Swap tuple elements. (Hashtag, count) =>
      .transform(_.sortByKey(false)) // Setting false for parameter: Ascending


    // Tophashtags60.print()

    // Print statement
    Tophashtags60.foreachRDD(rdd => {

      val storeList = rdd.take(10) // Store  and show only 10 rdd
      println("\n Popular hash tags during the last 60 seconds are : (%s total) ".format(rdd.count()))
      storeList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag,count))}
    })


    ssc.start() // Start streaming
    ssc.awaitTermination()
    //ssc.stop()


  }
}

/** TO DO : * ADD KAFKA INTEGRATION
  * CLEAN CODE
  * ADD SQL CONTEXT
  */


