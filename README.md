# MongoDB_Spark_Course
Code materials for the MongoDB Spark Course

Once Spark and MongoDB are up and running, and you've imported the dataset. You can check that everything is ready for the workshop with this following set of commands

## Execute me from the command line
```
./bin/spark-shell --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/nasa.eva" --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/nasa.astronautTotals" 
--packages org.mongodb.spark:mongo-spark-connector_2.10:0.1
```

## Once in the Spark Shell Execute this
```
import com.mongodb.spark._
import com.mongodb.spark.rdd.MongoRDD
sc.loadFromMongoDB().take( 10 ).foreach( println )
```

## Docker image with Spark and Mongo setup for the course
- Install docker
- `docker run -it -p27017:27017 -p8080:8080 -p7077:7077 nithanaroy/spark_mongo:0.1.1`
