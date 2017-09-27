import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by path2 on 23/05/17.
  */
object dum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TEST")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)


    val somelist = sc.parallelize((List(
      (0,"A"),
      (0,"A"),
      (1,"B"),
      (1,"A")))
      .map(row =>  ( (row._1,row._2,1) ) ))

    somelist.map{
      case ( (id:Int, predclass :String, value :Int) ) => ( (id,predclass), value)
    }.reduceByKey((vali,valacc) => vali + valacc ).collect().foreach(println)
  }

  /*
    val sales = sc.parallelize((List(("West",  "Apple",  7.0, 10),
      ("West",  "Apple",  3.0, 15),
      ("West",  "Orange", 5.0, 15),
      ("South", "Orange", 3.0, 9),
      ("South", "Orange", 6.0, 18),
      ("East",  "Milk",   5.0, 5) )))



    sales.map{

      case (store,prod, amt, units) => (
        (store, prod), (amt, units) // (amt, units) is tup1 (current) and tup2 (next).
      )}.reduceByKey((tup1,tup2) => (tup1._1 + tup2._1,tup1._2 + tup2._2 ) ).collect().foreach(println)
*/


  }





