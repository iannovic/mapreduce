package sample;

public class NodeHelper {
	
	public boolean updateGlobalNodes(Node n) {
		for (int i = 0; i < Global.nodes.size(); i++) {
			Node compare = Global.nodes.get(i);
			if (compare.getId() == n.getId()) {
				if (isEqualNode(compare,n)) {
					//System.out.println("they are the same =/ global structure will not be updated");
					return false;
				} else {
				//	System.out.println("You've updated your global structure!");
					Global.nodes.set(i, n);
					return true;
				}	
			}	
		}
		return false;
	}
	
	public boolean isEqualNode(Node n1, Node n2) {
		if (n1.getId() != n2.getId()) {
			return false;
		} else if (n1.getDistance() != n2.getDistance()) {
			return false;
		} else if (!n1.getList().equals(n2.getList())) {
			return false;
		}
		return true;
	}

}
