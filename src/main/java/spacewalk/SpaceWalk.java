package spacewalk;

import com.mongodb.spark.api.java.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

import org.bson.Document;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by brein on 4/18/2016.
 */
public class SpaceWalk implements Serializable {

    static Pattern pattern = Pattern.compile("(\\w+\\s\\w+)");

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.app.id", "MongoSparkConnectorTour")
                .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/nasa.eva")
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/nasa.astronautHours" );


        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);


        JavaPairRDD<String, Integer> logs = rdd.flatMapToPair(
                new PairFlatMapFunction<Document, String, Integer>
                () {
            @Override
            public Iterable< Tuple2<String, Integer>> call(Document document) throws Exception {
                int minutes = 0;
                String time = (String) document.get("Duration");
                if (!time.isEmpty()) {
                    String[] timeComponents = time.split(":");
                    minutes = (new Integer(timeComponents[0]) * 60);
                    minutes += ( new Integer(timeComponents[1]) );
                }

                String crewString = (String) document.get("Crew");
                Matcher matcher = pattern.matcher(crewString);
                List<String> crew = new ArrayList<String>();

                while (matcher.find()) {
                    crew.add(matcher.group());
                }

                final int finalMinutes = minutes;
                return crew.stream()
                        .map(new Function<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> apply(String c) {
                                return new Tuple2<>(c, finalMinutes);
                            }
                        })
                        .collect(Collectors.<Tuple2<String, Integer>>toList());
            }
        });

        JavaPairRDD<String, Integer> totalHours =  logs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        FlatMapFunction map =  new FlatMapFunction<Tuple2<String,Integer>, Document>(){
            @Override
            public List<Document> call(Tuple2<String, Integer> tuple) throws Exception {
                Document doc = new Document();
                doc.put( "_id", tuple._1 );
                doc.put( "minutes", tuple._2 );
                List <Document> docs = new ArrayList<Document>();
                docs.add( doc );
                return docs;
            }
        };

        JavaRDD<Document>  records  = totalHours.flatMap( map );

        totalHours.foreach(
                new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple ) throws Exception {
                        System.out.println( tuple._1 + " "+ tuple._2  );
                    }
                }
        );

        WriteConfig defaultWriteConfig = WriteConfig.create(jsc);
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "astronautHours");
        writeOverrides.put("databaseName", "nasa");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(writeOverrides, defaultWriteConfig);

        MongoSpark.save( records, writeConfig );

    }

}
