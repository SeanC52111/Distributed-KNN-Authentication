package quadIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;


public class Rect implements Writable{
	public double x1;
	public double x2;
	public double y1;
	public double y2;
	//constructor
	public Rect(double x1,double x2,double y1,double y2)
	{
		this.x1=x1;
		this.x2=x2;
		this.y1=y1;
		this.y2=y2;
	}
	
	public Rect() {
		
	}
	public Rect(Point p)
	{
		this.x1 = p.x;
		this.x2 = p.x;
		this.y1 = p.y;
		this.y2 = p.y;
	}
	//check whether r is conatained in Rect
	public boolean isContain(Rect r) {
		if(r.x1>=x1 && r.x1<=x2 && r.x2>=x1 && r.x2<=x2)
			if(r.y1>=y1 && r.y1<=y2 && r.y2>=y1 && r.y2<=y2)
				return true;
		return false;
	}
	
	public boolean isContain(Point p) {
		if(p.x>=x1 && p.x<=x2 && p.y>=y1 && p.y<=y2)
			return true;
		return false;
	}
	
	public Point getCenter() {
		return new Point((x1+x2)/2.0,(y1+y2)/2.0);
	}
	
	public boolean isIntersects(Rect r) {
		if(r.x2 < x1 || r.x1 > x2)
			return false;
		if(r.y1 > y2 || r.y2 < y1)
			return false;
		return true;
	}
	@Override
	public String toString() {
		String str = ""+x1+" "+x2+" "+y1+" "+y2;
		return str;
	}
	
	@Override
	public void readFields(DataInput in)throws IOException{
		x1 = in.readDouble();
		x2 = in.readDouble();
		y1 = in.readDouble();
		y2 = in.readDouble();
	}
	
	@Override
	public void write(DataOutput out)throws IOException{
		out.writeDouble(x1);
		out.writeDouble(x2);
		out.writeDouble(y1);
		out.writeDouble(y2);
	}
}
