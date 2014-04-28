package sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	private CentroidHelper centroid_helper = new CentroidHelper();
	public void reduce(Text key, Iterable<Data> values, Context context) throws IOException, InterruptedException {
		
		if(Global.centroid_list == null){
			Global.centroid_list = centroid_helper.populateCentroids();
		}
	/*******************************************NEW TWEET********************************************/
		//variables for new tweet
		Data tweet = TweetToData(key);
	/*************************************GET THE RIGHT CENTROID**********************************/
		//variables for centroid
		Data center = Global.centroid_list.get(tweet.getCluster_id());
	/*******************************************DO DA MATH*******************************************/
		while (values.iterator().hasNext()) {
			System.out.println(values.iterator().next().toString());
		}

		center.setCount(center.getCount()+1);
		center.setXval(center.getXval() * (center.getCount()-1) + tweet.getXval());
		center.setYval(center.getYval() * (center.getCount()-1) + tweet.getYval());
		center.setXval(center.getXval()/(center.getCount()));
		center.setYval(center.getYval()/(center.getCount()));
		
		context.write(key, result);
	}
	
	public Data TweetToData(Text t) {
		Data d = new Data();
		String k = t.toString();
		String keys[] = k.split("[:]");
		d.setCluster_id(Integer.parseInt(keys[1].trim()));
		d.setXval(Double.parseDouble(keys[2].trim()));
		d.setYval(Double.parseDouble(keys[3].trim()));
		d.setCount(1);
		return d;
	}
}
