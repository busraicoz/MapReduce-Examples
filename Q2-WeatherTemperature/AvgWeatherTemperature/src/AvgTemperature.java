import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgTemperature {

	public static class NcdcRecordParser {
		private static final int MISSING_TEMPERATURE = 9999;
		private String year;
		private int airTemperature;
		private String quality;

		public void parse(String record) {
			year = record.substring(15, 19);
			String airTemperatureString;
			
			if (record.charAt(87) == '+') {
				airTemperatureString = record.substring(88, 92);
			} else {
				airTemperatureString = record.substring(87, 92);
			}
			airTemperature = Integer.parseInt(airTemperatureString);
			quality = record.substring(92, 93);
		}

		public void parse(Text record) {
			parse(record.toString());
		}

		public boolean isValidTemperature() {
			return airTemperature != MISSING_TEMPERATURE
					&& quality.matches("[01459]");
		}

		public String getYear() {
			return year;
		}

		public int getAirTemperature() {
			return airTemperature;
		}
	}

	public static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
				context.write(new Text(parser.getYear()), new IntWritable(
						parser.getAirTemperature()));
			}
		}

	}

	public static class MaxTemperatureReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "mean temperature");
		job.setJarByClass(AvgTemperature.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
