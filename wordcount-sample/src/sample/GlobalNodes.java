package sample;

import java.util.LinkedList;

public class GlobalNodes {
	public static boolean is_first_iteration = true; //on the first iteration we handle the nodes structure differently in reduce.
	public static boolean has_changed = true; // as long as this variable is true, the output is changing, so keep iterating
	public static LinkedList<Node> nodes; //way of keeping track of the updated nodes
}
