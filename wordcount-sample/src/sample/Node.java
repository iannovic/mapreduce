package sample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Node implements Writable{

	private int id;
	private boolean is_node;
	private int distance;
	private ArrayList<Integer> list = new ArrayList<Integer>();
	@Override
	public void readFields(DataInput arg0) throws IOException {
		id = arg0.readInt();
		distance = arg0.readInt();
		is_node = arg0.readBoolean();
		String s = arg0.readLine();
		//System.out.println("Your line is :");
		//System.out.println("[" + s + "]");
		String ints[] = s.split("[:]");
		list = new ArrayList<Integer>();
		for (String i : ints) {
			i = i.trim();
			if (!i.equals("") && i.matches("[0-9]+$")) {
				list.add(Integer.parseInt(i));
			}
		}
		
	}
	public int getDistance() {
		return distance;
	}
	public void setDistance(int distance) {
		this.distance = distance;
	}
	public ArrayList<Integer> getList() {
		return list;
	}
	public void setList(ArrayList<Integer> list) {
		this.list = list;
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(id);
		arg0.writeInt(distance);
		arg0.writeBoolean(is_node);
		String s = "";
		for (int i = 0; i < list.size();i++) {
			s = s + list.get(i) + ":";
		}
		arg0.writeChars(s);
	}
	
	public int hashCode() {
		return ((Integer)id).hashCode();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	public boolean isIs_node() {
		return is_node;
	}
	public void setIs_node(boolean is_node) {
		this.is_node = is_node;
	}
	public String toString() {
		String ret = "";
		ret = ret + this.getId() + " " + this.getDistance() + " ";
		for (int i = 0; i < list.size(); i ++) {
			ret = ret + list.get(i) + ":";
		}
		return ret;
	}
}
