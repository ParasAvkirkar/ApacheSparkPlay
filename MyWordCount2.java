import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MyWordCount2 {
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(2);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), ";\"\'. \t‘“,");
			while (tok.hasMoreTokens()) {
				word.set(tok.nextToken());
				context.write(word, one);
			}
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			context.write(key, total);
		}
	}
	
	public static class MyReducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "step 1");
		myjob.setJarByClass(MyWordCount2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[1]));
		myjob.waitForCompletion(true);
		
		Job myjob2 = Job.getInstance(conf, "step 2");
		myjob2.setJarByClass(MyWordCount2.class);
		myjob2.setMapperClass(MyMapper.class);
		myjob2.setReducerClass(MyReducer2.class);
		myjob2.setOutputKeyClass(Text.class);
		myjob2.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(myjob2, new Path(args[1]));
		FileOutputFormat.setOutputPath(myjob2,  new Path(args[2]));
		myjob2.waitForCompletion(true);
		
		System.exit(0);
	}
}