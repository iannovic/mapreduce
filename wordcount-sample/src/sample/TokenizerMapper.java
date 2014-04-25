package sample;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
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
	//change this variable to get the desired output
	int mode = 0;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String s = word.toString();
			
			//this regex will match string s to be one of the original key's, 
			//because the new input to this mapreduce is both keys and values of a previous mapreduce.
			//once we have made a match, we need to iterate the white space until we have the value that matched with the key
			//and once we have that key,value, we must append the value until it has a uniform amount of digits as other values.
			
			switch(mode){
				case(0):
					if (s.charAt(0) == '@' || s.charAt(0) == '#' || s.contains("http")) {
					} else {
						s = s.replaceAll("[-0-9.:;\n\t\r#@, +?/!$&)(\"]*", "").toLowerCase().trim();	
						if(s != "" && s != null){context.write(word, one);}
					}
				break;
				case(1):
					if (s.charAt(0) == '#'){
						context.write(word, one);
					}
					break;
				case(2):
					if (s.charAt(0) == 'a'){
						context.write(word, one);
					}
					break;
				default:
					break;
				}
				
			}
	}
}