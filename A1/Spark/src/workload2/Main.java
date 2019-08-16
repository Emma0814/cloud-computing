package workload2;

import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Main {
	
	public static void main(String[] args) {
		String inputDataPath = args[0], outputDataPath = args[1];
	    SparkConf conf = new SparkConf();
	    conf.setAppName("Top 10 reverse growth videos Application");
	    JavaSparkContext sc = new JavaSparkContext(conf); 
	    Map<String, Boolean> videos = new HashMap<String, Boolean>();
	    JavaRDD<String> videoData = sc.textFile(inputDataPath);
        JavaRDD rdd = sc.parallelize(Arrays.asList(new Tuple2("0", 0)));
        JavaPairRDD pairRdd = JavaPairRDD.fromJavaRDD(rdd);
        JavaPairRDD<String, String> firstTrending = videoData.mapToPair(s -> {  
	    	if(!s.substring(0, 8).equals("video_id")) {
	    		String[] values = s.split(",");
	    		String video_id = values[0];
		    	String category = values[3];
		    	String diff = Integer.toString(Integer.parseInt(values[7]) - Integer.parseInt(values[6])); 
		    	String country = values[11];
		    	String key = video_id + "*" + country;
		    	if(!videos.containsKey(key)) {
		    		videos.put(key, true);
		    		return new Tuple2<String, String>(key, diff + "*" + category);
		    	}
	    	}
	    	return new Tuple2<String, String>("0", "0");
    	}); 
        firstTrending = firstTrending.subtractByKey(pairRdd);
	    JavaPairRDD<String, String> secondTrending = videoData.mapToPair(s -> {  
	    	if(!s.substring(0, 8).equals("video_id")) {
	    		String[] values = s.split(",");
		    	String video_id = values[0];
		    	String diff = Integer.toString(Integer.parseInt(values[7]) - Integer.parseInt(values[6])); 
		    	String country = values[11];
		    	String key = video_id + "*" + country;
		    	if(!videos.containsKey(key)) {
		    		videos.put(key, true);
		    	}else if(videos.containsKey(key) && videos.get(key)) {
		    		videos.put(key, false);
		    		return new Tuple2<String, String>(key, diff);
		    	}
	    	}
			return new Tuple2<String, String>("0", "0");
    	}); 
	    secondTrending = secondTrending.subtractByKey(pairRdd);
	    JavaPairRDD<String, Tuple2<String, String>> joinResults = firstTrending.join(secondTrending);
	    JavaPairRDD<String, Integer> calGrowth = joinResults.mapToPair(t -> { 
	    	int regex = t._2._1.indexOf("*");
	    	int diff = Integer.parseInt(t._2._2) - Integer.parseInt(t._2._1.substring(0, regex));
	    	return new Tuple2<String, Integer>(t._1 + "*" + t._2._1.substring(regex + 1), diff);		
	    });
	    calGrowth = calGrowth.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());
	    JavaRDD result = sc.parallelize(calGrowth.take(10));
	    result.map(l -> "\"" + l.toString().substring(1,12) + "\", "
	    		+ String.format("%-8s", l.toString().substring(l.toString().indexOf(",") + 1, l.toString().length() - 1) + ", ") 
	    		+ "\"" + String.format("%-20s", l.toString().substring(16, l.toString().indexOf(",")) + "\",") 
	    		+ "\"" + l.toString().substring(13, 15)+ "\"").saveAsTextFile(outputDataPath);
	}
}