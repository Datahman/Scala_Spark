// Load file to RDD

val auctionRDD = sc.textFile("file:///home/cloudera/Desktop/Spark/spark/auctiondata.csv")

// Split array elements at ","

auctionRDD.map(line => line.split(",")  )

// Show first RDD element
auctionRDD.first()

// Show first five elements
auctionRDD.take(5)

// Count each bid
auctionRDD.count()

// Count auctioned items

val total_auctioned_count = auctionRDD.map(line => line.split(",")).map(x => x(7)).distinct().count()

// XBOX total bids
 val XBOX_TOTAL_BIDS = auctionRDD.map(line => line.split(",")).filter(e => e(7) == "xbox").count()



// Naming data auction columns
val auctionid = 0;
val bid = 1;
val bidtime = 2;
val bidder = 3;
val bidderrate = 4
val openbid = 5;
val price = 6;
val itemtype = 7;
val daystolive = 8;

// Work on sfpd dataset


val incidentNum=0;
val cat=1;
val dayofWeek=3;
val date=4;
val time=5;
val pdDist=6;
val res =7;
val address=8;
val x=9;
val y=10;
val pdID = 11;
val sfpd  = sc.textFile("file:///home/vagrant/workspace/Spark/spark/sfpd.csv").map(line => line.split(","));
sfpd.first();

val totincidents = sfpd.count();
// Collect categories
val Categories = sfpd.map(i => i(cat)).distinct().collect();


http://backtobazics.com/big-data/spark/apache-spark-reducebykey-example/

//testarray. Input array (1, 2, 3, 4)

testarray.map(dx => (if(dx==1) {var string_ = "test" ;( dx,s"$string_")} 
else {var string_ = "test2";( dx,s"$string_")}) )

// Find top 5 addresses with most incidents

 val address_with_top_incidents = sfpd.map(x => (x(address),1)).reduceByKey((a,b) => (a+b)).map(x => (x._2,x._1) ).sortByKey(false).take(5);

// Find top 3 Categories with most incidents

sfpd.map(x => (x(cat),1)).reduceByKey((a,b) => (a+b)).map(x => (x._2,x._1) ).sortByKey(false).take(3);

// Find incidents at each district

sfpd.map(x => (x(pdDist),1)).reduceByKey((a,b) => (a+b) ).map(x => (x._2, x._1)).sortByKey(false).collect();



val AddCat_Data = sc.textFile("file:///home/cloudera/Desktop/Spark/spark/AddCat.csv").map(line => line.split(","));
val AddDist_Data = sc.textFile("file:///home/cloudera/Desktop/Spark/spark/AddDist.csv").map(line => line.split(",")); 


AddCat_Data.map(x => (x(1),x(0) ));
AddDist_Data.map(x => (x(1),x(0) ));
 ABOVE GIVES AN ARRAY OF TUPLES WHICH WONT WORK ON SPARK SQL !

NOTE: TODF doesnt work on tuples BUT ON CASE CLASSES !


TO DO
https://www.infoq.com/articles/apache-spark-sql


## USE SPARK SQL TO CHANGE RDD TO DF TYPE  ##

val sqlContext = new org.apache.spark.sql.SQLContext(sc) // Create the SQLContext first from the existing Spark Context
//val sqlContext = new SQLContext(sc)
import sqlContext.implicits._ // Import statement to implicitly convert an RDD to a DataFrame

//  Create case class for District-Address dataset
case class ADDClass(District: String, Address: String)

//  Create case class for Incident-Address dataset
case class IncidentClass(Incident_Type: String, Address: String)

// MAKE DFs for each dataset
val AddDist_Data = sc.textFile("file:///home/cloudera/Desktop/Spark/spark/AddDist.csv").map(line => line.split(",")).map(x => (ADDClass(x(0), x(1) ) )).toDF() 

val Incident_Data = sc.textFile("file:///home/cloudera/Desktop/Spark/spark/AddCat.csv").map(line => line.split(",")).map(x => (IncidentClass(x(0), x(1) ) )).toDF() 


// Register DFs as tables
AddDist_Data.registerTempTable("District-Address")
Incident_Data.registerTempTable("Incident-Address")

// Display table contents
Incident_Data.show()
AddDist_Data.show()

// Display table schema
Incident_Data.printSchema()
AddDist_Data.printSchema()

alias.join(Incidents,"Address").show()



NOTE: sc need to be defined on scala repl


// Other work 


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._


 val my_join = sqlContext.sql("SELECT a.Address , a.District, i.Incident_Type FROM AddDist_Data a INNER JOIN Incident_Data i ON a.Address=i.Address").show()


// 

val data = Array("coffee 45", "coffee 20", "coffee 20" , "coffee 32", "tea 48", "tea 32");

val Drink = 0;
val Age = 1;

val rdd = sc.parallelize(data).map(s => s.split(" "))

val d1 = rdd.map(eacharray => (eacharray(Drink),(eacharray(Age).toFloat,1) ) );
d1.collect();

val d2 = d1.reduceByKey((ta,tc) => (ta._1 + tc._1, ta._2 + tc._2) )
// Note: d1 out put is a tuple t: (t1,t2). ReduceByKey has an accumulator(ta) and counter(tc).
// d2 returns array of tuples (String,(Float,Int)) tuple within tuple
val d3 = d2.map( t => (t._1, (t._2._1 / t._2._2)));
d3.collect()

// Note ._ operator
//Note: t._2._1 represents second element of outer tuple and ._2 the inner tuple second element. 


case class Case(Drink: String, Age:Float)
// Change to DF
val Data = d1.map(d => (Case(d._1, d._2._1))).toDF()

Data.show()

Data.registerTempTable("BeverageTable")


// Find average age of the drinker
sqlContext.sql("SELECT AVG(b.Age)AS Average_Age, b.Drink FROM BeverageTable b  GROUP BY b.Drink").show()

// Auction dataframe

val auction = sc.textFile("file:///home/cloudera/Desktop/Spark/spark/auctiondata.csv").map(line => line.split(","));

case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate: Int, openbid: Float, finprice: Float, itemtype: String, dtl: Int)

val auctions = auction.map(a => Auction(a(0), a(1).toFloat, a(2).toFloat, a(3), a(4).toInt, a(5).toFloat, a(6).toFloat, a(7), a(8).toInt)).toDF()

auctions.registerTempTable("AuctionTable")

sqlContext.sql("SELECT * FROM AuctionTable LIMIT 5").show()

// Find total bid count per user
sqlContext.sql("SELECT COUNT(a.bid), a.bidder FROM AuctionTable a GROUP BY a.bidder ORDER BY COUNT(a.bid) desc").show()

// Find auction price finish less than 30.

sqlContext.sql("SELECT COUNT(*) FROM AuctionTable a WHERE a.finprice < 30").show()

// To do

// SEE total items a particular user has bid on.

sqlContext.sql("SELECT  a.itemtype FROM AuctionTable a WHERE a.bidder like '%warrencheryl%' GROUP BY a.itemtype  ").show()


// Distinct item types
sqlContext.sql("SELECT  a.itemtype FROM AuctionTable a GROUP BY a.itemtype  ").show()

// Average bid per item. Assuming non-repetitive /// WRONG

 sqlContext.sql("SELECT a.itemtype, COUNT(*)/COUNT(DISTINCT a.bidder) AS average  FROM AuctionTable a GROUP BY a.itemtype").show()

// MAKE DATA FRAMES FOR EACH ITEM
 val xboxDF = sqlContext.sql("SELECT * FROM AuctionTable a WHERE a.itemtype like '%xbox%' ")
 val palmDF = sqlContext.sql("SELECT * FROM AuctionTable a WHERE a.itemtype like '%palm%' ")
 val cartierDF = sqlContext.sql("SELECT * FROM AuctionTable a WHERE a.itemtype like '%cartier%' ")


// Maximum / Average / Minimum price for each item

val statsxboxDF = xboxDF.describe("finprice").show()
val palmDFDF = palmDF.describe("finprice").show()
val cartierDFDF = cartierDF.describe("finprice").show()

// SFPD DF




val totincidents = sfpd.count();

case class PoliceCase(incidentNum: Float, cat: String,desc: String,dayofWeek: String,date: String,time: String,pdDist: String,reason: String,address: String,xcord: Float,ycord: Float,pdID: Float)





val sfpdDF  = sfpd.map(a => PoliceCase(a(0).toInt,a(1), a(2), a(3),a(4), a(5), a(6), a(7), a(8), a(9).toFloat, a(10).toFloat, a(11).toFloat)).toDF();

sfpdDF.registerTempTable("SFPD TABLE")



/*
Using spark functionality find the following:

CASE (A): Top 5 incidents with most number of incidents
		  Use cat as key value on ReduceByKey method
 
*/
 sfpd.map(eachelement => (eachelement(0),1)).reduceByKey(_+_).sortBy(_._2,false).take(5)
 val Top_10_incidents = sfpd.map(eachelement => (eachelement(7),1)).reduceByKey(_+_).sortBy(_._2,false).take(10)
 sfpd.map(eachelement => (eachelement(1),1)).reduceByKey(_+_).sortBy(_._2,false).take(3)



// Save output to JSON

Top_10_incidents.write.json("file:///home/vagrant/workspace/WK8/Spark/spark/Top10-Incidents-Output.txt")

// Retreive current working directory
// def pwd = System.getProperty("user.dir"); println(pwd)
/* Save DF to JSON
write.json(file:///<FILENAME>)
*/
// Making Spark application


https://youtu.be/9mELEARcxJo?t=2727



sqlContext.sql("SELECT COUNT(s.incidentNum) AS INCIDENTS_PER_DATE, s.date FROM SFPD s WHERE s.date ( SELECT s.date FROM s WHERE dayofWeek = s.dayofWeek)  GROUP BY s.date LIMIT 10 ").show();

// In scala use double escape character twice

//  Regular expression to extract first word only
val stringpattern = "(\\w+)".r
//


// Register function as a UDF to use on SparkSQL statements



// SQL query using UDF defined above

sqlContext.sql(" SELECT TitleCount(Name) FROM TitanicTitles LIMIT 10").show()

// Case class to store Name column only 
case class TitanicTitles(Name: String)
// Load data and split at comma
val titanicdata = sc.textFile("file:///home/vagrant/workspace/WK8/Spark/spark/train.csv").map(a => a.split(","))
// Exclude column names array
val cleandata = titanicdata.filter(!_.contains("Sex")).map(a => stringpattern.findFirstIn(a(4)))

// Extract only array content from Option[String] using get method
val cleandataDF = cleandata.map(a => TitanicTitles(a.get)).toDF()
// Verify case class is loaded by checking the schema
cleandataDF.printSchema
// Register dataframe as a SQL table
cleandataDF.registerTempTable("TitanicTitles")

/*
val Counter = (row: String) => {
	var EmptyArray = Array.empty[String];
	var x = Emptyarray +: (row); x.head;
	}

*/

/*
val Counter = (row: String ) => {val BA = ArrayBiffer[String](); BA += stringpattern.findFirstIn(row).get}
*/

/*
EXERSICE UDF

BUILD A UDF TO COUNT THE OCCURENCE OF STRING "MR" ON THE TITANIC DATA SET column name
*/
var Counter =0
def myFunc : (String  => Int ) = {
	row => {
		if (row == "Mr") {
			Counter +=1; Counter // Note return value must be assignment (Counter), else scala throws Unit()
		}
		else {0}
	}

}
// Load UDF module
import org.apache.spark.sql.functions.udf
// Register UDF name to bsed used on the SQL query.
sqlContext.udf.register("CountMrTitle", myFunc)


// SQL query using UDF

sqlContext.sql("SELECT MAX(CountMrTitle(Name)) FROM TitanicTitles").show()

// Answer: 274 Instances of title "Mr"

