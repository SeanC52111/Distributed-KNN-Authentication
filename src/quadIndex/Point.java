package quadIndex;


public class Point {
	public double x;
	public double y;
	
	public Point(double x,double y)
	{
		this.x = x;
		this.y = y;
	}
	public boolean isInside(Rect r) {
		if(x>= r.x1 && x <= r.x2)
			if(y >= r.y1 && y <= r.y2)
				return true;
		return false;
	}
}