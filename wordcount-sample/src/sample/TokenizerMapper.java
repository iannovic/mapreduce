package sample;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String s = word.toString();
			if (s.charAt(0) == '@' || s.charAt(0) == '#' || s.contains("http")) {
			} else {
				s = s.replaceAll("[.:,?!\"]", " ").toLowerCase().trim();	
				word.set(s);
				context.write(word, one);
			}
		}
	}
}