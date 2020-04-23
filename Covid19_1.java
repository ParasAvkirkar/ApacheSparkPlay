

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_1 {
	//TODO: Remove unnecessary methods
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class CaseMapper extends Mapper<Object, Text, Text, CountryStatWritable> {

		public static boolean isWorld;
		
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
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer columnTokens = new StringTokenizer(value.toString(), ",");
			String columns[] = new String[4];
			int i = 0;
			while (i < 4 && columnTokens.hasMoreTokens()) {
				columns[i] = columnTokens.nextToken();
				++i;
			}
		
			// processing only when non header rows encounterd
			if (!"date".equals(columns[0])) {
				if (!isWorld && "world".equals(columns[1].toLowerCase())) {
					return;
				}
				
				Date date = parseDate(columns[0]);
				Date oldDateFilter = parseDate("2019-12-31");
				// considering dates only after Dec, 12, 2019
				if (date != null && oldDateFilter != null && date.after(oldDateFilter)) { 
					// created a Writable class to hold the country-name and case-counts
					CountryStatWritable stat = new CountryStatWritable(columns[1], Long.parseLong(columns[2]));
					context.write(new Text(columns[1]), stat);
				}
			}
		}
		
	}
	
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class CountReducer extends Reducer<Text, CountryStatWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<CountryStatWritable> values, Context context) throws IOException, InterruptedException {
			long totalCases = 0;
			for (CountryStatWritable stat: values) {
				totalCases += stat.getCaseCount().get();
			}
			
			total.set(totalCases);
		
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		boolean isWorld = Boolean.parseBoolean(args[1]);
		CaseMapper.isWorld = isWorld;

		Configuration conf = new Configuration();
		Job jobInstance = Job.getInstance(conf, "Total count of reported cases");
		
		jobInstance.setJarByClass(Covid19_1.class);
		jobInstance.setMapperClass(CaseMapper.class);
		jobInstance.setReducerClass(CountReducer.class);

		jobInstance.setMapOutputKeyClass(Text.class);
		jobInstance.setMapOutputValueClass(CountryStatWritable.class);

		jobInstance.setOutputKeyClass(Text.class);
		jobInstance.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobInstance,  new Path(args[2]));
		System.exit(jobInstance.waitForCompletion(true) ? 0 : 1);
	}
}

class CountryStatWritable implements Writable {
	
	private Text countryName;
	private LongWritable caseCount;
	
	public CountryStatWritable(String countryName, long caseCount) {
		this.countryName = new Text(countryName);
		this.caseCount = new LongWritable(caseCount);
	}

	public CountryStatWritable() {
		this.countryName = new Text();
		this.caseCount = new LongWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		countryName.write(out);
		caseCount.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		countryName.readFields(in);
		caseCount.readFields(in);
	}

	public Text getCountryName() {
		return countryName;
	}

	public void setCountryName(Text countryName) {
		this.countryName = countryName;
	}

	public LongWritable getCaseCount() {
		return caseCount;
	}

	public void setCaseCount(LongWritable caseCount) {
		this.caseCount = caseCount;
	}

	@Override
	public String toString() {
		return "CountryStat [countryName=" + countryName + ", caseCount=" + caseCount + "]";
	}
}