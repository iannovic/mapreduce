package sample;

import java.io.IOException;
//import java.util.StringTokenizer;


import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable(1);
	private Text word = new Text("test");
	private Text main_key = new Text("");

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String keys = key.toString().replaceAll("\\[", ",").replaceAll("\\]",",");
		String splitKeys[] = keys.split("[,]");
		LinkedList<String> key_list = new LinkedList<String>();
		int size = splitKeys.length;
		word.set(keys.trim());
		result.set(1);
		context.write(word,result);
		for (int i = 0; i < size-1; i ++) {
			if (i==0 && size > 0) {
				main_key.set(splitKeys[i].trim());
			} else if (!splitKeys[i].trim().equals("")){
				key_list.add(splitKeys[i].trim());
			}
		}
		
		for (int i = 0; i < key_list.size(); i++) {
			word.set(main_key.toString() + "-" + key_list.get(i));
			result.set(1);
			context.write(word,result);
		}
	}
}
