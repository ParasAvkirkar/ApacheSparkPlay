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

public class Covid19_1 {

    public static class CaseMapper extends Mapper<Object, Text, Text, LongWritable> {
        private LongWritable caseCount = new LongWritable();
        private Date parseDate(String date) {
            Date result = null;

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            try {
                result = format.parse(date);
            } catch (ParseException e) {
                // no handles
            }

            return result;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String columns[] = value.toString().trim().split(",");

            // processing only when non header rows encountered
            if (!"date".equals(columns[0])) {
                // get boolean parameter set in configuration from the command-line args
                boolean isWorld = context.getConfiguration().getBoolean("isWorld", true);
                if ("world".equals(columns[1].toLowerCase()) || "international".equals(columns[1].toLowerCase())) {
                    // if the current row is World row or International row
                    // ignore it if the isWorld is set to false
                    if (!isWorld) {
                        return;
                    }
                }

                Date date = parseDate(columns[0]);
                Date oldDateFilter = parseDate("2019-12-31");
                // considering dates only after Dec, 12, 2019
                if (date != null && oldDateFilter != null && date.after(oldDateFilter)) {
                    // created a Writable class to hold the country-name and case-counts
                    caseCount.set(Long.parseLong(columns[2]));
                    context.write(new Text(columns[1]), caseCount);
                }
            }
        }

    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable total = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long totalCases = 0;
            for (LongWritable caseCount: values) {
                totalCases += caseCount.get();
            }

            total.set(totalCases);

            context.write(key, total);
        }
    }


    public static void main(String[] args)  throws Exception {
        boolean isWorld = Boolean.parseBoolean(args[1]);

        Configuration conf = new Configuration();
        conf.setBoolean("isWorld", isWorld);

        Job jobInstance = Job.getInstance(conf, "Total count of reported cases");

        jobInstance.setJarByClass(Covid19_1.class);
        jobInstance.setMapperClass(CaseMapper.class);
        jobInstance.setReducerClass(CountReducer.class);

        jobInstance.setMapOutputKeyClass(Text.class);
        jobInstance.setMapOutputValueClass(LongWritable.class);

        jobInstance.setOutputKeyClass(Text.class);
        jobInstance.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobInstance,  new Path(args[2]));
        System.exit(jobInstance.waitForCompletion(true) ? 0 : 1);
    }
}

