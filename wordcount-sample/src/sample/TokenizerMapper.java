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
			if (s.matches("[A-Za-z'@#]+$")) {
				boolean keepLooping = true;
				while (keepLooping) {
					if (itr.hasMoreTokens()) {
						String d = itr.nextToken();
						if (d.matches("[0-9]+$")) {
							while (d.length() < 7) {
								d = '0' + d;
							}
							d = d + " " + s;
							word.set(d);
							keepLooping = false;
						}
					} else {
						keepLooping = false;
					}
				}
				context.write(word, one);
			}

		}
	}
}