package sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private final int WORD = 0;
	private final int HASH = 1;
	private final int MENTION =2;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		int mode = 0;
		ArrayList<String> wordList = new ArrayList<String>();
		int c = 0;
		whileLoop:
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String s = word.toString();
				switch(c){
				case(0):
					if(s.equals("Sun") || s.equals("Mon") || s.equals("Tue")
							|| s.equals("Wed") || s.equals("Thu") || s.equals("Fri")
							|| s.equals("Sat")){
						c = 1;
					}
				break;
				case(1):
					if(s.equals("Apr")){
						c++;
					}else{
						if(s.equals("Sun") || s.equals("Mon") || s.equals("Tue")
								|| s.equals("Wed") || s.equals("Thu") || s.equals("Fri")
								|| s.equals("Sat")){
							c = 1;
						}else{c=0;}
					}
				break;
				case(2):
					if(s.contains("0") || s.contains("1") || s.contains("2") || s.contains("3")){
						c = 0;
						for(int i = 0; i < 2; i++){wordList.remove(wordList.size() - 1);}
						for(int i = 0; i < 5; i++){
							if(itr.hasMoreTokens()){
								itr.nextToken();
							}else{break whileLoop;}
						}
						write(wordList, context);
						wordList.clear();
					}else{
						if(s.equals("Sun") || s.equals("Mon") || s.equals("Tue")
								|| s.equals("Wed") || s.equals("Thu") || s.equals("Fri")
								|| s.equals("Sat")){
							c = 1;
						}else{c=0;}
					}
				break;
				default:
				break;
			}
			switch(mode){
				case(WORD):
					if (s.charAt(0) == '@' || s.charAt(0) == '#' || s.contains("http")) {
					} else {
						s = s.replaceAll("[-0-9.:;\n\t\r#@, +?/!$&)(\"]*", "").toLowerCase().trim();	
						if(s != "" && s != null){wordList.add(s);}
					}
				break;
				case(HASH):
					if (s.charAt(0) == '#'){
						wordList.add(s);
					}
					break;
				case(MENTION):
					if (s.charAt(0) == 'a'){
						wordList.add(s);
					}
					break;
				default:
					break;
				}	
		}
		write(wordList, context);
	}
	
	private void write(ArrayList<String> wordList, Context context) {
		
		for(int i = 0; i < wordList.size() - 1; i++){
			for(int j = 0; j < wordList.size() - 1; j++){
				if(!(i==j) && wordList.get(i) != null && wordList.get(j) != null && wordList.get(i) != "" && wordList.get(j) != ""){
					String first = wordList.get(i);
					String second = wordList.get(j);
					try {
						word.set(first + "-" + second);
						context.write(word, one);
						word.set(first + "-*");
						context.write(word, one);
					} catch (IOException | InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}


