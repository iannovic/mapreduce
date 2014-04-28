package sample;

import java.util.LinkedList;

public class Global {
	public static boolean has_changed = true; // as long as this variable is true, the output is changing, so keep iterating
	public static boolean first_iteration = true; //for populating the linkedlist on the first iteration
	public static LinkedList<Node> nodes; //way of keeping track of the updated nodes
	public static boolean is_complete; //racks to see if the job is complete
}
