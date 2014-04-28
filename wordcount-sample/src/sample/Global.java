package sample;

import java.util.LinkedList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Global {
	public static FileSystem fs;
	public static Path p;
	public static LinkedList<Data> centroid_list;
}
