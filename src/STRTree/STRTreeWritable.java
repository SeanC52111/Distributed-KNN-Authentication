package STRTree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import debug.Debug;
import quadIndex.Rect;

public class STRTreeWritable extends STRTree implements Writable{
	public STRTreeWritable()
	{
		super(null,0);
	}
	public STRTreeWritable(ArrayList<Rect> rectlist,int nodec) {
		super(rectlist,nodec);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException{
		//Debug.println("TreeWritable::readFields");
		int size = in.readInt();
		//Debug.println("data size: "+ size);
		ArrayList<STRNode> nl=new ArrayList<STRNode>();
		for(int i=0;i<size;i++) {
			Rect r = new Rect();
			r.readFields(in);
			nl.add(new STRNode(r,true,null,""));
		}
		this.nodelist = nl;
		root = readSubTree(in);
	}
	
	private STRNode readSubTree(DataInput in) throws IOException{
		boolean is_null = in.readBoolean();
		if(is_null) {
			Rect r = new Rect();
			r.readFields(in);
			boolean isleaf = in.readBoolean();
			Text h = new Text();
			h.readFields(in);
			return new STRNode(r,isleaf,null,h.toString());
		}
		else {
			Rect r = new Rect();
			r.readFields(in);
			boolean isleaf = in.readBoolean();
			Text h = new Text();
			h.readFields(in);
			int size = in.readInt();
			ArrayList<STRNode> arr = new ArrayList<STRNode>();
			for(int i=0;i<size;i++) {
				arr.add(readSubTree(in));
			}
			return new STRNode(r,isleaf,arr,h.toString());
		}
	}
	@Override
	public void write(DataOutput out) throws IOException{
		//Debug.println("TReeWritable");
		int size = nodelist.size();
		out.writeInt(size);
		//Debug.println(size);
		Rect r = new Rect();
		for(int i=0;i<size;i++)
		{
			r = nodelist.get(i).MBR;
			r.write(out);
			//Debug.println(r.x1+" "+r.x2+" "+r.y1+" "+r.y2);
		}
		writeSubTree(root,out);
	}
	private void writeSubTree(STRNode root,DataOutput out) throws IOException{
		if(root.child == null) {
			out.writeBoolean(true);
			//Debug.println("root child is null-leafnode");
			root.MBR.write(out);
			//Debug.println("mbr "+root.MBR.toString());
			out.writeBoolean(root.isleaf);
			//Debug.println("is leaf "+root.isleaf);
			Text h =new Text(root.hashvalue);
			h.write(out);
			//Debug.println("hash value "+h.toString());
		}
		else {
			out.writeBoolean(false);
			//Debug.println("has root child");
			root.MBR.write(out);
			//Debug.println("mbr "+root.MBR.toString());
			out.writeBoolean(root.isleaf);
			//Debug.println("is leaf "+root.isleaf);
			Text h =new Text(root.hashvalue);
			h.write(out);
			//Debug.println("hash value "+h.toString());
			out.writeInt(root.child.size());
			for(int i=0;i<root.child.size();i++) {
				writeSubTree(root.child.get(i),out);
			}
		}
	}
}
