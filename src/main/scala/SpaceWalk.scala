import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql._
import org.apache.spark.rdd.RDD

val defaultReadConfig = ReadConfig(sc);

val readConfig = ReadConfig( Map("collection" -> "astronautHours" ) )

"database" -> "nasa", "readPreference.name" ->
  "secondaryPreferred"),
  Some(defaultReadConfig)
)

val rdd = sc.loadFromMongoDB();

println ( rdd.first );

case class EVALog ( "id #": String, Country: String, Crew: String, Vehicle: String, Date: String, Duration: String,
Purpose: String )

val sqlContext = new SQLContext(sc);
val EVASchema = sqlContext.read.mongo();
EVASchema.printSchema()

val EVASchema = sqlContext.read.mongo[EVALog]();

def flatMapToDurations( mongoRDD: MongoRDD ) : RDD = {


}
val EVADataframe = sqlContext.loadFromMongoDB().toDF[EVALog]()

val durations = rdd.flatMap( )