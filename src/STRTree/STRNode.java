package STRTree;
import quadIndex.Rect;
import java.util.*;


public class STRNode {
	public Rect MBR=null;
	public boolean isleaf=true;
	public ArrayList<STRNode> child=null;
	public String hashvalue;
	public STRNode(Rect mbr,boolean isleaf,ArrayList<STRNode> child,String hashvalue) {
		this.MBR = mbr;
		this.isleaf = isleaf;
		this.child = child;
		this.hashvalue = hashvalue;
	}
}