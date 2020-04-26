import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/* Spark imports */

public class SparkCovid19_1 {

    // Date-parsing helper method for Driver and Mapper methods
    private static Date parseDate(String date) {
        Date result = null;

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            result = format.parse(date);
        } catch (ParseException e) {
            // no handles
        }

        return result;
    }

    // Validate date-formats and the range of date
    private static boolean isDateRangeValid(String start, String end) {
        Date startDate = parseDate(start);
        Date endDate = parseDate(end);

        Date startFilter = parseDate("2019-12-31");
        Date endFilter = parseDate("2020-04-08");

        if (startDate != null && endDate != null && startFilter != null && endFilter != null) {

            if (startDate.before(startFilter) || endDate.after(endFilter)) {
                // if the startDate is before the earliest day or the endDate is later than latest date
                return false;
            }

            // if start day comes before end-date or they both represent same day
            return startDate.before(endDate) || startDate.equals(endDate);
        }

        return false;
    }

    // This is implementation of Task-2 in the earlier part
    public static void main(String[] args) {
        boolean isDateValid = isDateRangeValid(args[1], args[2]);
        final Date startDate = parseDate(args[1]);
        final Date endDate = parseDate(args[2]);

        if (isDateValid) {
            /* essential to run any spark code */
            SparkConf conf = new SparkConf().setAppName("SparkCovid19_1");
            JavaSparkContext sc = new JavaSparkContext(conf);

            /* load input data to RDD */
            JavaRDD<String> dataRDD = sc.textFile(args[0]);

            JavaPairRDD<String, Long> totalCasesByCountry = dataRDD.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String row) throws Exception {
                    // Skipping header row
                    if (row.contains("location")) {
                        return false;
                    }

                    Date date = parseDate(row.split(",")[0]);
                    // Skipping rows outside given date range
                    if (date.before(startDate) || date.after(endDate)) {
                        return false;
                    }

                    return true;
                }
            }).flatMapToPair(new PairFlatMapFunction<String, String, Long>() {

                @Override
                public Iterator<Tuple2<String, Long>> call(String value) throws Exception {
                    String[] columns = value.split(",");

                    List<Tuple2<String, Long>> resultStat = new ArrayList<>();
                    resultStat.add(new Tuple2<>(columns[1], Long.parseLong(columns[3])));

                    return resultStat.iterator();
                }
            }).reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long first, Long second) throws Exception {
                    return first + second;
                }
            }).sortByKey();

            totalCasesByCountry.saveAsTextFile(args[3]);
        } else {
            System.out.println("Given date-format and range is not valid");
        }
    }
}