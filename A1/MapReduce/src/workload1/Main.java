package workload1;

import java.io.*;
import java.text.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	public static class MyMapper extends Mapper<Object, Text, Text, Text>{
		private Text category = new Text();
		private Text video_idAndCountry  = new Text();	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataArray = value.toString().split(",");
			String video_idStr = dataArray[0];
			String categoryStr = dataArray[3];
			String countryStr = dataArray[11];
			if(categoryStr.equals("category")) {
				return;
			}
			category.set(categoryStr);
			video_idAndCountry.set(video_idStr + "*" + countryStr + "=1,");
			context.write(category, video_idAndCountry);
		}
	}

	public static class MyReducer extends Reducer<Text,Text,Text,Text> {
		Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, List<String>> videoAndCountry = new HashMap<String, List<String>>();
			for (Text text: values){
				String[] dataArray = text.toString().split(",");
				for(int i = 0; i < dataArray.length; i++) {
					String video_id = dataArray[i].substring(0, 11);
					String country = dataArray[i].substring(12, 14);
					if(!videoAndCountry.containsKey(video_id)) {
						List<String> countries = new LinkedList<>();
						countries.add(country);
						videoAndCountry.put(video_id, countries);
					}else if(!videoAndCountry.get(video_id).contains(country)) {
						List<String> countries = videoAndCountry.get(video_id);
						countries.add(country);
						videoAndCountry.put(video_id, countries);
					}
				}
			}
			NumberFormat formatter = new DecimalFormat("#0.00"); 
			double count = 0.00;
			for (String video_id : videoAndCountry.keySet()){
				count += videoAndCountry.get(video_id).size();
			}
			result.set("");
			key.set(key.toString() + ": " + formatter.format(count / videoAndCountry.size()));
			context.write(key, result);
		 }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();		
		conf.set("mapreduce.textoutputformat.separatorText", ": ");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Main <in> <out>");
			System.exit(2);
		}	
		Job job = new Job(conf, "average country number of for videos in each category");
		job.setJarByClass(Main.class);		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
