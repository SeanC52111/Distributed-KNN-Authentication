package MapReduceTask;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.*;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import quadIndex.Point;
import quadIndex.Rect;

public class STRTreeMain {
	public static void main(String []args)throws IOException,Exception {
		/*
		if(args.length<1)
		{
			System.out.println("invalid syntax.");
			return ;
		}
		String sub_cmd = args[0];
		String[] arguments = Arrays.copyOfRange(args, 1, args.length);
		if(sub_cmd.equals("index")) {
			STRTreeIndex.run(arguments);
		}else if(sub_cmd.equals("range_query")) {
			STRTreeRangeQuery.run(arguments);
		}
		
		else {
			System.out.println("Umrecognized sub commands.");
		}
		*/
		String [] index = {"input","tree"};
		String [] rangequery = {"1,1,7,-1,5","tree","output"};
		String[] knn = {"1,1,7,4","tree","output"};
		//STRTreeIndex.run(index);
		//STRTreeRangeQuery.run(rangequery);
		
		
		STRTreeKNN.run(knn);
		String knnresult = getkNN("hdfs://localhost:9000/user/hadoop/output/part-00000");
		int check = knnresult.indexOf("needrange");
		System.out.println("check" +check);
		
		
		if(check != -1) {
		String[] mbr = knnresult.split("\t")[1].split(" ");
		String knnrange = "2,"+mbr[1]+","+mbr[2]+","+mbr[3]+","+mbr[4];
		try {
			deleteDir("output");
		}catch(Exception e) {}
		String [] knnrangequery = {knnrange,"tree","output"};
		STRTreeRangeQuery.run(knnrangequery);
		
		Comparator<Rect> rectcmp = new Comparator<Rect>() {
			public int compare(Rect r1,Rect r2) {
				double dist1 = getMinimumDist(new Point(4,2),r1);
				double dist2 = getMinimumDist(new Point(4,2),r2);
				if(dist1 < dist2) {
					return -1;
				}
				else if(dist1 > dist2)
					return 1;
				else
					return 0;
			}
		};
		ArrayList<String> volist = new ArrayList<String>();
		ArrayList<String> rootsig = new ArrayList<String>();
		Queue<Rect> result = new PriorityQueue<Rect>(11,rectcmp);
		ArrayList<String> filecontent = new ArrayList<String>();
		filecontent = getFileContent("hdfs://localhost:9000/user/hadoop/output/part-00000");
		for(String con : filecontent) {
			System.out.println(con);
			if(con.indexOf("VO")!=-1) {
				volist.add(con);
			}
			else if(con.indexOf("root_sig")!=-1) {
				rootsig.add(con);
			}
			else {
				mbr = con.split("\t")[1].split(" ");
				result.add(new Rect(Double.valueOf(mbr[0]),Double.valueOf(mbr[1]),Double.valueOf(mbr[2]),Double.valueOf(mbr[3])));
			}
		}
		
		for(int i=0;i<2;i++) {
			System.out.println(result.poll().toString());
		}
		
		}
		
		
	}
	
	private static double getMinimumDist(Point q,Rect r) {
		double ret = 0.0;
		if(q.x < r.x1)
			ret += Math.pow(r.x1-q.x, 2);
		else if(q.x > r.x2)
			ret += Math.pow(q.x-r.x2, 2);
		if(q.y < r.y1)
			ret += Math.pow(r.y1-q.y, 2);
		else if(q.y > r.y2)
			ret += Math.pow(q.y-r.y2, 2);
		return ret;
	}
	
	private static ArrayList<String> getFileContent(String uri)throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		InputStream in=null;
		ArrayList<String> ret = new ArrayList<String>();
		try {
			in = fs.open(new Path(uri));
			BufferedReader  br = new BufferedReader(new InputStreamReader(in));
			while(br.ready()) {
				String line = br.readLine();
				ret.add(line);
			}
			br.close();
		}
		finally {
			
		}
		return ret;
	}
	private static String getkNN(String uri)throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		InputStream in=null;
		String knn = "";
		try {
			in = fs.open(new Path(uri));
			BufferedReader  br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			knn = line;
			br.close();
		}
		finally {
			
		}
		return knn;
	}
	private static void deleteDir(String s)throws Exception{
		Configuration conf = new Configuration();
		Path output = new Path(s);
		FileSystem fs = output.getFileSystem(conf);
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
	}
}
