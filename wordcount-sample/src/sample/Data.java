package sample;

public class Data {
	
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
}
