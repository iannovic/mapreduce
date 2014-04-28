package sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, Node>{
	/*
	 * this version of the mapper will SORT any previous wordcount based on the value.
	 * the idea is simple; we concanate the value of the <key,value> onto the front of the key.
	 * the new key is made to be a concanated 
	 * 			string {oldvalue + " " + oldkey} = newkey
	 * mapreduce will automatically sort these keys to be in increasing order by value.
	 * 
	 * we had to add one work around to this code, and that was to append zeroes to each value until they all
	 * were of the same size, we chose the number 7 because we will never run a mapreduce job of more than 100,000 tweets
	 * so a maximum of 7 digits suits the size of our jobs, a word will never appear more than one million times.
	 */
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Node node = new Node();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String nodeId = word.toString();
			System.out.println(nodeId);
			String distance = "";
			String adj = "";
			if (nodeId.matches("[^:]")) {
				if (itr.hasMoreTokens()) {
					distance = itr.nextToken().toString().trim();
					System.out.println(distance);
				}
				if (distance.matches("[^:]")) {
					if (itr.hasMoreTokens()) {
							adj = itr.nextToken().toString();
							System.out.println(adj);
					}
				}	
			}
			
			//emit (nid n, N);
			String adj_list[] = adj.split("[:]");
			ArrayList<Integer> list = new ArrayList<Integer>();
			for (String s : adj_list) {
				list.add(Integer.parseInt(s.trim()));
			}
			node.setId(Integer.parseInt(nodeId));
			node.setIs_node(true);
			node.setDistance(Integer.parseInt(distance));
			node.setList(list);
			context.write(word, node);
			
			/*for all nodeid m existing in adj list ... emit */
			node.setIs_node(false);
			node.setDistance(node.getDistance()+1);
			for (int i = 0; i < list.size(); i ++){
				node.setId(list.get(i));
				word.set(list.get(i).toString());
				context.write(word, node);
			}
		}
	}
}