import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class SparkCovid19_2 {

    private static HashMap<String, Long> readCountryPopulation(String filePath, JavaSparkContext sparkContext) {
        JavaRDD<String> rddCache = sparkContext.textFile(filePath);

        Map<String, Long> map = rddCache.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String columns[] = s.split(",");

                // Skipping invalid rows
                if (columns.length < 5) {
                    return false;
                }

                // Skipping header row
                if ("location".equals(columns[1].toLowerCase())) {
                    return false;
                }

                // Skipping 0 population rows
                long population = Long.parseLong(columns[4]);
                if (population <= 0) {
                    return false;
                }

                return true;
            }
        }).flatMapToPair(new PairFlatMapFunction<String, String, Long>() {

            @Override
            public Iterator<Tuple2<String, Long>> call(String value) throws Exception {
                String[] columns = value.split(",");

                List<Tuple2<String, Long>> resultStat = new ArrayList<>();
                resultStat.add(new Tuple2<>(columns[1], Long.parseLong(columns[4])));

                return resultStat.iterator();
            }
        }).collectAsMap();

        return new HashMap<>(map);
    }

    // This is implementation of Task-3 in the earlier part
    public static void main(String[] args) {
        /* essential to run any spark code */
        SparkConf conf = new SparkConf().setAppName("SparkCovid19_2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        HashMap<String, Long> populationMap = readCountryPopulation(args[1], sc);
        final Broadcast<HashMap<String, Long>> broadcastWrapper = sc.broadcast(populationMap);


        /* load input data to RDD */
        JavaRDD<String> dataRDD = sc.textFile(args[0]);

        JavaPairRDD<String, Double> output = dataRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String row) throws Exception {
                // Skipping header row
                if (row.contains("location")) {
                    return false;
                }
                // Returning true for only those countries having corresponding population available
                HashMap<String, Long> countryPopulation = broadcastWrapper.value();
                return countryPopulation.containsKey(row.split(",")[1]);
            }
        }).flatMapToPair(new PairFlatMapFunction<String, String, Long>() {

            @Override
            public Iterator<Tuple2<String, Long>> call(String value) throws Exception {
                String[] columns = value.split(",");

                List<Tuple2<String, Long>> resultStat = new ArrayList<>();
                resultStat.add(new Tuple2<>(columns[1], Long.parseLong(columns[2])));

                return resultStat.iterator();
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long first, Long second) throws Exception {
                return first + second;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Long>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Long> countryCasePair) throws Exception {
                HashMap<String, Long> countryPopulation = broadcastWrapper.getValue();
                double population = countryPopulation.get(countryCasePair._1);
                double casesPerMillion = (countryCasePair._2 * 1000000.0) / population;

                return new Tuple2<>(countryCasePair._1, casesPerMillion);
            }
        }).sortByKey();

        output.saveAsTextFile(args[2]);
    }
}