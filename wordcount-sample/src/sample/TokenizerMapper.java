package sample;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, Data>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Data data = new Data();
	private CentroidHelper centroid_helper = new CentroidHelper();
	

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());

		/*****************************GET THE FOLLOWER AND FRIEND COUNT OF THE NEW TWEET*******************************************/
		//int index = 0;
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String s = word.toString();
			Data tweet = new Data();
			String keys[] = s.split("[,;]");
			
			if (keys.length == 2) {
				Double d = Double.parseDouble(keys[0]);
				tweet.setXval(d);
				d = Double.parseDouble(keys[1]);
				tweet.setYval(d);
			}
		
			/*****************FIND THE DISTANCE TO ALL CENTROIDS AND PICK BEST CENTER*****************************/
			//read from file into the String "centers"
			//File Written in the format: "a:xval,yval,coutn;b:xval,yval;c:xval,yval,count;........"

			//Shortest Distance
			double sd = 1000000000;
			double newDistance = 1000000000;

			if (Global.centroid_list == null) {
				Global.centroid_list = centroid_helper.populateCentroids();
			}
			
			//In this loop we get all of the centers from the "centers" string and see if each one is a better match
			for (int i = 0; i < Global.centroid_list.size(); i ++) {
				Data center = Global.centroid_list.get(i);
				newDistance = getDistance(tweet.getXval(),tweet.getYval(),center.getXval(),center.getYval());
				if(newDistance < sd){
					sd = newDistance;
					tweet.setCluster_id(center.getCluster_id());
				}
			}

			/*************************EMIT**************************************/			
			word.set(":" + tweet.getCluster_id() + ":" + tweet.getXval() + ":" + tweet.getYval() + ":");
			data.setXval(tweet.getXval());
			data.setYval(data.getYval());
			data.setCount(1);
			data.setCluster_id(tweet.getCluster_id());
			context.write(word, data);
		}

	}
	public double getDistance(double xValOne, double yValOne, double xValTwo, double yValTwo){
		double xDist = Math.abs(xValOne - xValTwo);
		double yDist = Math.abs(yValOne - yValTwo);
		xDist = xDist*xDist;
		yDist = yDist*yDist;
		return Math.sqrt(xDist+yDist);
	}
}

