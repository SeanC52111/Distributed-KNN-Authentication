package test;
import STRTree.*;
import java.util.*;
import java.io.*;
import Tool.Hasher;
import quadIndex.*;

public class test {
	public static void main(String []args) {
		ArrayList<Rect> rlist = new ArrayList<Rect>();
		
		try {
			FileReader fw = new FileReader("datapoints.txt");
			BufferedReader bf = new BufferedReader(fw);
			String s = null;
			while((s = bf.readLine())!=null) {
				String[] split = s.split(" ");
				double x1 = Double.valueOf(split[0]);
				double x2 = Double.valueOf(split[1]);
				double y1 = Double.valueOf(split[2]);
				double y2 = Double.valueOf(split[3]);
				rlist.add(new Rect(x1,x2,y1,y2));
				//System.out.println(x1+" "+x2+" "+y1+" "+y2);
			}
		}
		catch(IOException e) {
			System.out.println(e.getMessage());
		}
		
		STRTree strtree = new STRTree(rlist,3);
		LinkedList<String> VO = new LinkedList<String>();
		LinkedList<Rect> result = new LinkedList<Rect>();
		strtree.secureKNN(4, new Point(3,15), result, VO);
		System.out.println("results");
		for(Rect r:result) {
			System.out.println(r.toString());
		}
		System.out.println("VOs");
		for(String s:VO) {
			System.out.println(s);
		}
		VOreturn voreturn = strtree.RootHash(VO);
		System.out.println(voreturn.hash.equals(strtree.root.MBR.toString()+strtree.root.hashvalue));
	}
}
