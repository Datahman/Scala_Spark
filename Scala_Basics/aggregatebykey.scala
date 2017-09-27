package retail

/**
  * Created by path2 on 14/05/17.
  * Demonstration of the aggregateByKey function
  * Problem statement: Get the total revenue per day for all completed,
  * and closed orders.
  *
  * First: Filter orders for COMPLETE and CLOSED
  */

import org.apache.spark.{SparkContext, SparkConf}

object dailyrevenue {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Daily Revenue"))

  val orders = sc.textFile("orders.csv")
  orders.first
  orders.take(100).foreach(println)
  val ordersFiltered = orders.filter(input => input.split(",")(3) == "COMPLETE" || input.split(",")(3) == "CLOSED")


    /* Sample work using aggregateByKey function.
    * Similar to reduceByKey. Requires initialisers (curry function), and implementation of two funciton based
    * operations (i) map phase, () reduce phase.
    * Can perform multiple key related operations, dependent on initial conditions.
    */

  /*
   * Consider the following list:
  */

  val SomeList = sc.parallelize(List(("A",2),("B",1),("A",2),("B",3)))

  val SumKeyValyeAndKeyOccurence = SomeList.aggregateByKey((0.0,0))( (aggrTuple, elemval) => (elemval+aggrTuple._1, aggrTuple._2+1) , (comba1,comba2) => (comba1._1 + comba2._1, comba1._2+comba2._2) )


}

