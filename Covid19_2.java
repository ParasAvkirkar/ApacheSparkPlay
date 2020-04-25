import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

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
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class DeathCaseMapper extends Mapper<Object, Text, Text, CountryStatWritable> {
		
		private Date startDate;
		private Date endDate;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, CountryStatWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			
			startDate = parseDate(context.getConfiguration().get("startDate"));
			endDate = parseDate(context.getConfiguration().get("endDate"));
		}

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
				Date date = parseDate(columns[0]);
				
				if (date != null) {
					// skip dates which are outside the filter
					if (date.after(endDate) || date.before(startDate)) {
						return;
					}
				}
				
				CountryStatWritable stat = new CountryStatWritable(columns[1], Long.parseLong(columns[3]));
				context.write(new Text(columns[1]), stat);
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
			long totalDeathCounts = 0;
			for (CountryStatWritable stat: values) {
				totalDeathCounts += stat.getDeathCount().get();
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
			jobInstance.setMapOutputValueClass(CountryStatWritable.class);

			jobInstance.setOutputKeyClass(Text.class);
			jobInstance.setOutputValueClass(LongWritable.class);
			
			FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
			FileOutputFormat.setOutputPath(jobInstance,  new Path(args[3]));
			System.exit(jobInstance.waitForCompletion(true) ? 0 : 1);
		} else {
			System.out.println("Invalid date-format and range is given");
		}
	}
}

class CountryStatWritable implements Writable {
	
	private Text countryName;
	private LongWritable deathCount;
	
	public CountryStatWritable(String countryName, long deathCount) {
		this.countryName = new Text(countryName);
		this.deathCount = new LongWritable(deathCount);
	}

	public CountryStatWritable() {
		this.countryName = new Text();
		this.deathCount = new LongWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		countryName.write(out);
		deathCount.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		countryName.readFields(in);
		deathCount.readFields(in);
	}

	public Text getCountryName() {
		return countryName;
	}

	public LongWritable getDeathCount() {
		return deathCount;
	}

	@Override
	public String toString() {
		return "CountryStat [countryName=" + countryName + ", deathCount=" + deathCount + "]";
	}
}