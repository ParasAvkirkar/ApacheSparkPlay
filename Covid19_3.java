import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Covid19_3 {

    // 4 types declared: Type of input key, type of input value, type of output key, type of output value
    public static class CaseMapper extends Mapper<Object, Text, Text, CountryStatWritable> {

        private String[] parseLineIntoColumns(StringTokenizer columnTokens) {
            String columns[] = new String[4];
            int i = 0;
            while (i < 4 && columnTokens.hasMoreTokens()) {
                columns[i] = columnTokens.nextToken();
                ++i;
            }

            return columns;
        }

        // The 4 types declared here should match the types that was declared on the top
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer columnTokens = new StringTokenizer(value.toString(), ",");

            String columns[] = parseLineIntoColumns(columnTokens);

            // processing only when non header rows encountered
            if (!"date".equals(columns[0])) {
                CountryStatWritable stat = new CountryStatWritable(columns[1], Long.parseLong(columns[2]));
                context.write(new Text(columns[1]), stat);
            }
        }

    }


    // 4 types declared: Type of input key, type of input value, type of output key, type of output value
    // The input types of reduce should match the output type of map
    public static class CaseReducer extends Reducer<Text, CountryStatWritable, Text, DoubleWritable> {
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

        // Notice the that 2nd argument: type of the input value is an Iterable collection of objects
        //  with the same type declared above/as the type of output value from map
        public void reduce(Text key, Iterable<CountryStatWritable> values, Context context) throws IOException, InterruptedException {
            String countryName = key.toString();
            if (populationMap.containsKey(countryName)) {
                long totalCases = 0;
                for (CountryStatWritable stat: values) {
                    totalCases += stat.getCaseCount().get();
                }

                casesPerMillPopulation.set((1.0 * totalCases * 1000000)/populationMap.get(countryName));
                context.write(key, casesPerMillPopulation);
            }
        }
    }

    // A Writable class to hold the data to be written by mapper
    private static class CountryStatWritable implements Writable {

        private Text countryName = new Text();
        private LongWritable caseCount = new LongWritable();

        public CountryStatWritable(String countryName, long caseCount) {
            this.countryName = new Text(countryName);
            this.caseCount = new LongWritable(caseCount);
        }

        public CountryStatWritable() { }

        @Override
        public void write(DataOutput out) throws IOException {
            countryName.write(out);
            caseCount.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            countryName.readFields(in);
            caseCount.readFields(in);
        }

        public LongWritable getCaseCount() {
            return caseCount;
        }

        @Override
        public String toString() {
            return "CountryStat [countryName=" + countryName + ", caseCount=" + caseCount + "]";
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
        jobInstance.setMapOutputValueClass(CountryStatWritable.class);

        jobInstance.setOutputKeyClass(Text.class);
        jobInstance.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobInstance,  new Path(args[2]));
        System.exit(jobInstance.waitForCompletion(true) ? 0 : 1);
    }
}