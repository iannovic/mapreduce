package sample;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	private CentroidHelper centroid_helper = new CentroidHelper();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		if(Global.centroid_list == null){
			Global.centroid_list = centroid_helper.populateCentroids();
		}
		/*
		 * We are passed from the mapper a key of this form: [bestCenterName]xVal,yVal
		 * 
		 * We read from the file, which is in this form: name1:xVal,yVal,count;name2:xVal,yVal,count;......
		 * 
		 */
	/*******************************************NEW TWEET********************************************/
		//variables for new tweet
		Data tweet = TweetToData(key);
		
		/*check to see if the cluster is different from the previous reduce() */
	/*************************************GET THE RIGHT CENTROID**********************************/
		//variables for centroid
		Data center = Global.centroid_list.get(tweet.getCluster_id());
		
	/*******************************************DO DA MATH*******************************************/
		

		center.setXval(center.getXval() * (center.getCount()) + tweet.getXval());
		center.setYval(center.getYval() * (center.getCount()) + tweet.getYval());
		center.setCount(center.getCount()+1);
		center.setXval(center.getXval()/(center.getCount()));
		center.setYval(center.getYval()/(center.getCount()));
	/*****************************************WRITE TO FILE*****************************************/	
		/*
		 * We have the old contents of the file stored in the string "centroids"
		 * We also have the index of the beginning of the centroid we want in the int "cIndex"
		 * Now, we need to delete the old contents of xVal, yVal, and count, but leave everything else the same
		 * Note - calling bw.write() will write over the contents of the file
		 * Do not change the value of cIndex past this point
		 */
		//This doesn't really do anything, our output will be written in the data/cenrtoids.txt file
		//centroid_helper.writeToFile(Global.centroid_list);
		context.write(key, result);
	}
	
	public Data TweetToData(Text t) {
		Data d = new Data();
		
		String k = t.toString();
		String keys[] = k.split("[:]");
		if (keys.length == 3) {
			d.setCluster_id(Integer.parseInt(keys[0].trim()));
			d.setXval(Double.parseDouble(keys[1].trim()));
			d.setYval(Double.parseDouble(keys[2].trim()));
		}
		return d;
	}
}
