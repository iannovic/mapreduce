package sample;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	//change this variable to get the desired output
	int mode = 0;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String s = word.toString();
			switch(mode){
				case(0):
					if (s.charAt(0) == '@' || s.charAt(0) == '#' || s.contains("http")) {
					} else {
						s = s.replaceAll("[-0-9.:;\n\t\r#@, +?/!$&)(\"]*", "").toLowerCase().trim();	
						if(s != "" && s != null){context.write(word, one);}
					}
				break;
				case(1):
					if (s.charAt(0) == '#'){context.write(word, one);}
					break;
				case(2):
					if (s.charAt(0) == 'a'){
						context.write(word, one);}
					break;
				default:
					break;
				}
		}
	}
}