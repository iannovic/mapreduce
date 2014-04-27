package sample;

import java.io.IOException;
//import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer 
extends Reducer<Text,IntWritable,Text,DoubleWritable> {
	private DoubleWritable result = new DoubleWritable(1);
	private DoubleWritable total = new DoubleWritable(0);
	private Text word = new Text("test");
	private Text current= new Text("nothing_quite_yet");
	private HashMap<String,Integer> map = new HashMap<String,Integer>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String keys = key.toString().replaceAll("\\[", ",").replaceAll("\\]",",");
		String splitKeys[] = keys.split("[,]");
		int size = splitKeys.length;
		
		//word.set(keys.trim());
		//result.set(1);
		//context.write(word,result);
		
		for (int i = 0; i < size; i ++) {
			if (i==0 && size > 0) {
				String s = splitKeys[i].trim().replaceAll("[,]", "");
				if (!s.equals(current.toString()))	{

					int sum = getMapTotal();
					total.set(sum);
		
					Set<String> key_set = map.keySet();
					for (String set : key_set) {
						word.set(current.toString() + "-" + set);
						result.set((double)(map.get(set)/total.get()));
						context.write(word,result);
					}
					map.clear();
					current.set(s);
					total.set(0);
				}
			} else if (!splitKeys[i].trim().equals("")){
				String s = splitKeys[i].trim().replaceAll("[,]", "");
				if (map.containsKey(s)) {
					map.put(s, map.get(s) + 1);	
				} else {
					map.put(s, 1);
				}
			}
		}
		
		
	}
	
	public int getMapTotal() {
		int sum = 0;
		Set<String> key_set = map.keySet();
		for (String set : key_set) {
			sum += map.get(set);
		}
		return sum;
	}
}
