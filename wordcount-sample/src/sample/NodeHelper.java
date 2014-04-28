package sample;

public class NodeHelper {
	
	public boolean updateGlobalNodes(Node n) {
		boolean ret = false;

		for (int i = 0; i < GlobalNodes.nodes.size(); i++) {
			Node compare = GlobalNodes.nodes.get(i);
			if (compare.getId() == n.getId()) {
				if (compare.equals(n)) {
					System.out.println("they are the same =/ global structure will not be updated");
					ret = false;
				} else {
					System.out.println("You've updated your global structure!");
					ret = true;
					GlobalNodes.nodes.set(i, n);
				}	
			}	
		}		
		return ret;
	}

}
