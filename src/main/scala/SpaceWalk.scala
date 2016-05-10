import com.mongodb.spark._
import com.mongodb.spark.rdd.MongoRDD
import org.bson.Document

val rdd = sc.loadFromMongoDB()

println( rdd.count() )
println ( rdd.first() )
for( doc <- rdd.take( 10 ) ) println( doc )

import com.mongodb.spark.config._

val readConf = ReadConfig( sc )
val readConfig = ReadConfig( Map("collection" -> "eva" ), Some(readConf))
val newRDD = sc.loadFromMongoDB( readConfig = readConfig )

def  breakoutCrew (  document: Document  ): List[(String,Int)]  = {

  var minutes = 0;
  val timeString = document.get( "Duration").asInstanceOf[String]
  if( timeString != null && !timeString.isEmpty ) {
    val time =  document.get( "Duration").asInstanceOf[String].split( ":" )
    minutes = time(0).toInt * 60 + time(1).toInt
  }

  import scala.util.matching.Regex
  val pattern = new Regex("(\\w+\\s\\w+)")
  val names =  pattern findAllIn document.get( "Crew" ).asInstanceOf[String]
  var tuples : List[(String,Int)] = List()
  for ( name <- names ) { tuples = tuples :+ (( name, minutes ) ) }

  return tuples
}

val logs = rdd.flatMap( breakoutCrew ).reduceByKey( (m1: Int, m2: Int) => ( m1 + m2 ) )

logs.foreach( println )

val writeConf = WriteConfig(sc)
val writeConfig = WriteConfig(Map("collection" -> "astronautTotals", "writeConcern.w" -> "majority", "db" -> "nasa"), Some(writeConf))

def mapToDocument( tuple: (String, Int )  ): Document = {
  val doc = new Document();
  doc.put( "name", tuple._1 )
  doc.put( "minutes", tuple._2 )

  return doc
}

logs.map( mapToDocument ).saveToMongoDB( writeConfig )

import org.apache.spark.sql.SQLContext
import com.mongodb.spark.sql._

// load the first dataframe "EVAs"
val evadf = sqlContext.read.mongo()
evadf.printSchema()
evadf.registerTempTable("evas")

// load the 2nd dataframe "astronautTotals"
case class astronautTotal ( name: String, minutes: Integer )
val astronautDF = sqlContext.read.option("collection", "astronautTotals").mongo[astronautTotal]()
astronautDF.printSchema()
astronautDF.registerTempTable("astronautTotals")

sqlContext.sql("SELECT astronautTotals.name, astronautTotals.minutes FROM astronautTotals"  ).show()


sqlContext.sql("SELECT astronautTotals.name, astronautTotals.minutes, evas.Vehicle, evas.Duration FROM " +
  "astronautTotals JOIN evas ON astronautTotals.name LIKE evas.Crew"  ).show()


