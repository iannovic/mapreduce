package sample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Data implements Writable{
	
	private int cluster_id;
	private int count;
	
	private double xval;
	private double yval;
	
	private boolean done = false;

	public double getXval() {
		return xval;
	}
	
	public void setXval(double xval) {
		this.xval = xval;
	}
	public double getYval() {
		return yval;
	}
	public void setYval(double yval) {
		this.yval = yval;
	}
	
	public int getCluster_id() {
		return cluster_id;
	}
	public void setCluster_id(int cluster_id) {
		this.cluster_id = cluster_id;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public boolean isDone() {
		return done;
	}
	public void setDone(boolean done) {
		this.done = done;
	}
	
	public String toString() {
		String ret;
		ret = this.getCluster_id() + ":" + this.getXval() + ":" + this.getYval() + ":" + this.getCount();
		return ret;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		cluster_id = arg0.readInt();
		count = arg0.readInt();
		xval = arg0.readDouble();
		yval = arg0.readDouble();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(cluster_id);
		arg0.writeInt(count);
		arg0.writeDouble(xval);
		arg0.writeDouble(yval);
		
	}
}
