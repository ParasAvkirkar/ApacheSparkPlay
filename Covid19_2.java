import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Covid19_2 {

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

    public static class DeathCaseMapper extends Mapper<Object, Text, Text, LongWritable> {
        private LongWritable deathCount = new LongWritable();
        private Date startDate; // start-date from input command
        private Date endDate; // end-date from input command

        @Override
        protected void setup(Mapper<Object, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);

            startDate = parseDate(context.getConfiguration().get("startDate"));
            endDate = parseDate(context.getConfiguration().get("endDate"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().trim().split(",");

            // processing only when non header rows encountered
            if (!"date".equals(columns[0])) {
                Date date = parseDate(columns[0]);

                if (date != null) {
                    // skip dates which are outside the filter
                    if (date.after(endDate) || date.before(startDate)) {
                        return;
                    }
                }
                deathCount.set(Long.parseLong(columns[3]));

                context.write(new Text(columns[1]), deathCount);
            }
        }

    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable total = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long totalDeathCounts = 0;
            for (LongWritable deathCount: values) {
                totalDeathCounts += deathCount.get();
            }

            total.set(totalDeathCounts);

            context.write(key, total);
        }
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

     public static void main(String[] args)  throws Exception {
        if (isDateRangeValid(args[1], args[2])) {
            Configuration conf = new Configuration();
            conf.set("startDate", args[1]);
            conf.set("endDate", args[2]);

            Job jobInstance = Job.getInstance(conf, "Total count of death cases");

            jobInstance.setJarByClass(Covid19_2.class);
            jobInstance.setMapperClass(DeathCaseMapper.class);
            jobInstance.setReducerClass(CountReducer.class);

            jobInstance.setMapOutputKeyClass(Text.class);
            jobInstance.setMapOutputValueClass(LongWritable.class);

            jobInstance.setOutputKeyClass(Text.class);
            jobInstance.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
            FileOutputFormat.setOutputPath(jobInstance,  new Path(args[3]));
            System.exit(jobInstance.waitForCompletion(true) ? 0 : 1);
        } else {
            System.out.println("Given date-format and range is not valid");
        }
    }
}