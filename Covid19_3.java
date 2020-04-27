import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Covid19_3 {

    public static class CaseMapper extends Mapper<Object, Text, Text, LongWritable> {
        private LongWritable count = new LongWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String columns[] = value.toString().trim().split(",");

            // processing only when non header rows encountered
            if (!"date".equals(columns[0])) {
                count.set(Long.parseLong(columns[2]));
                context.write(new Text(columns[1]), count);
            }
        }

    }

    public static class CaseReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {
        private DoubleWritable casesPerMillPopulation = new DoubleWritable();
        private Map<String, Long> populationMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            FileSystem hdfsFileSystem = FileSystem.get(context.getConfiguration());
            URI[] cacheFiles = context.getCacheFiles();

            Path path = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(hdfsFileSystem.open(path)));

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                line = line.trim();
                String[] columns = line.split(",");

                if (columns.length < 5) {
                    continue;
                }
                if ("location".equals(columns[1].toLowerCase())) {
                    // Skipping header
                    continue;
                }

                long population = Long.parseLong(columns[4]);
                if (population > 0) {
                    populationMap.put(columns[1], population);
                }
            }
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String countryName = key.toString();
            if (populationMap.containsKey(countryName)) {
                double totalCases = 0;
                for (LongWritable caseCount: values) {
                    totalCases += caseCount.get();
                }

                casesPerMillPopulation.set((totalCases * 1000000)/populationMap.get(countryName));
                context.write(key, casesPerMillPopulation);
            }
        }
    }

    public static void main(String[] args)  throws Exception {
        Configuration conf = new Configuration();

        Job jobInstance = Job.getInstance(conf, "Number of Cases per million population");

        jobInstance.addCacheFile(new Path(args[1]).toUri());

        jobInstance.setJarByClass(Covid19_3.class);
        jobInstance.setMapperClass(CaseMapper.class);
        jobInstance.setReducerClass(CaseReducer.class);

        jobInstance.setMapOutputKeyClass(Text.class);
        jobInstance.setMapOutputValueClass(LongWritable.class);

        jobInstance.setOutputKeyClass(Text.class);
        jobInstance.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobInstance,  new Path(args[2]));
        System.exit(jobInstance.waitForCompletion(true) ? 0 : 1);
    }
}