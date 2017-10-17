package MapReduceTask;
import java.net.URI;
import java.util.Arrays;
import java.util.*;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import quadIndex.Point;
import quadIndex.Rect;

public class STRTreeTestkNN {
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
		String inputname = "mapinput";
		String localindex = "tree";
		String globaltablename = "globaltable";
		String knnlocaloutput = "knnlocal";
		String refineoutput = "refine";
		int nodecapcity = 6;
		int k = 1;
		Point q = null;
		ArrayList<GlobalRecord> list = readGlobalIndex(globaltablename);
		q = new Point(22.49,113.45);
		String knnpartition = findkNNPartition(q,list);
		if(knnpartition.equals("")) {
			System.out.println("q is not inside the data range");
			return;
		}
		//localkNN(1,q,k,knnpartition,localindex,knnlocaloutput);
		String strq = String.valueOf(q.x)+","+String.valueOf(q.y);
		String[] knnquery = {"1"+","+"1"+","+strq,"tree"+"/"+"1"+"-r-00000","refine"};
		STRTreeKNN.run(knnquery);		

					
		}
	
		
		/*
		
		//createIndex(inputname,localindex,6,globaltablename);
		
		
		//read globalindex into memory
		ArrayList<GlobalRecord> list = readGlobalIndex(globaltablename);
		//run local knn
		//Point q = new Point(22.4967766,113.4526999);
		//Point q = new Point(7,4);
		
		String knnpartition = findkNNPartition(q,list);
		if(knnpartition.equals("")) {
			System.out.println("q is not inside the data range");
			return;
		}
		localkNN(1,q,k,knnpartition,localindex,knnlocaloutput);
		String refine = checkRefinement(knnlocaloutput);
		if(refine !="") {
			System.out.println("refinement");
			refine(refine,localindex,refineoutput,list); 
		}else {
			System.out.println("local knn is the final knn");
			Configuration conf = new Configuration();
			//conf.set("fs.default.name", "hdfs://localhost:9000");
			InputStream in=null;
			ArrayList<String> reslist = new ArrayList<String>();
			try {
				FileSystem fs = FileSystem.get(conf);
				Path path = new Path("/user/hadoop/"+knnlocaloutput+"/part-00000");
				in = fs.open(path);
				BufferedReader  br = new BufferedReader(new InputStreamReader(in));
				while(br.ready()) {
					String line = br.readLine();
					reslist.add(line+"\n");
				}
				
				br.close();
			}catch(Exception e) {}
			for(GlobalRecord l:list) {
				if(l.filename!=knnpartition) {
					reslist.add("UnprocessedVO\t"+l.mbr.toString()+" "+l.hash+"\n");
				}
			}
			FileWriter fw = new FileWriter("knnresult.txt",false);
			for(String r:reslist) {
				fw.write(r);
			}
			fw.close();
			
		}
		*/
		
		/*

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
		*/
	
	
	
	public static void refine(String refine,String localindex,String refineoutput,ArrayList<GlobalRecord> list) {
		String[] mbr = refine.split("\t")[1].split(" ");
		String knnrange = "0,"+mbr[1]+","+mbr[2]+","+mbr[3]+","+mbr[4];
		Rect r = new Rect(Double.valueOf(mbr[1]),Double.valueOf(mbr[2]),Double.valueOf(mbr[3]),Double.valueOf(mbr[4]));
		try {
			Configuration conf = new Configuration();
			//conf.set("fs.default.name", "hdfs://localhost:9000");
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/user/hadoop/"+refineoutput);
			if(fs.exists(path)) {
				System.out.println("delete duplicate "+refineoutput);
	        	fs.delete(path,true);
	        }
		}catch(Exception e) {}
		ArrayList<String> rangelist = findRangeQueryPartition(r,list);
		ArrayList<String> rangefilelist = new ArrayList<String>();
		
		for(String str:rangelist) {
			rangefilelist.add(localindex+"/"+str+"-r-00000");
		}
		String rangesearchinput = String.join(",", rangefilelist);
		System.out.println(rangesearchinput);
		String [] knnrangequery = {knnrange,rangesearchinput,refineoutput};
		STRTreeRangeQuery.run(knnrangequery);
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "hdfs://localhost:9000");
		InputStream in=null;
		ArrayList<String> reslist = new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/user/hadoop/"+refineoutput+"/part-00000");
			in = fs.open(path);
			BufferedReader  br = new BufferedReader(new InputStreamReader(in));
			while(br.ready()) {
				String line = br.readLine();
				reslist.add(line+"\n");
			}
			
			br.close();
		}catch(Exception e) {}
		for(GlobalRecord l:list) {
			if(!rangelist.contains(l.filename)) {
				reslist.add("UnprocessedVO\t"+l.mbr.toString()+" "+l.hash+"\n");
			}
		}
		try {
			FileWriter fw = new FileWriter("knnresult.txt",false);
			for(String res:reslist) {
				fw.write(res);
			}
			fw.close();
		}catch(Exception e) {}
		
	}
	
	
	public static String checkRefinement(String knnlocaloutput) {
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "hdfs://localhost:9000");
		InputStream in=null;
		String result = "";
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/user/hadoop/"+knnlocaloutput+"/part-00000");
			in = fs.open(path);
			BufferedReader  br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			if(line.indexOf("needrange")!=-1) {
				result = line;
			}
			br.close();
		}catch(Exception e) {}
		return result;
	}
	public static void localkNN(int qid,Point q,int k,String knnpartition,String localindex,String output) {
		String strqid = String.valueOf(qid);
		String strk = String.valueOf(k);
		String strq = String.valueOf(q.x)+","+String.valueOf(q.y);
		String[] knnquery = {strqid+","+strk+","+strq,localindex+"/"+knnpartition+"-r-00000",output};
		STRTreeKNN.run(knnquery);
	}
	
	
	
	public static void createIndex(String input,String output,int nodec,String globaltablename) {
		String[] index = {input,output,String.valueOf(nodec)};
		try {
			MultiOutputIndex.run(index);
			Configuration conf = new Configuration();
			//conf.set("fs.default.name", "hdfs://localhost:9000");
	        FileSystem fs = FileSystem.get(conf);
			Path redunfile = new Path("/user/hadoop/"+output+"/part-r-00000");
	        if(fs.exists(redunfile)) {
	        	//System.out.println("yes");
	        	fs.delete(redunfile,true);
	        }
	        String[] globalindex = {output,globaltablename};
			GlobalIndex.run(globalindex);//construct globalindex table
		}catch(Exception e) {e.printStackTrace();}
	}
	
	
	
	public static ArrayList<GlobalRecord> readGlobalIndex(String globaltable) {
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "hdfs://localhost:9000");
		ArrayList<GlobalRecord> list = new ArrayList<GlobalRecord>();
		InputStream in=null;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/user/hadoop/"+globaltable+"/part-r-00000");
			in = fs.open(path);
			BufferedReader  br = new BufferedReader(new InputStreamReader(in));
			while(br.ready()) {
				String line = br.readLine();
				String filename = line.split("\t")[0];
				String mbrhash = line.split("\t")[1];
				String[] split = mbrhash.split(" ");
				double x1 = Double.valueOf(split[0]);
				double x2 = Double.valueOf(split[1]);
				double y1 = Double.valueOf(split[2]);
				double y2 = Double.valueOf(split[3]);
				String hash = split[4];
				Rect MBR = new Rect(x1,x2,y1,y2);
				list.add(new GlobalRecord(filename,MBR,hash));
			}
			br.close();
		}catch(Exception e) {}
		
		return list;
	}
	
	
	
	public static String findkNNPartition(Point q,ArrayList<GlobalRecord> list) {
		String ret = "";
		for(GlobalRecord l:list) {
			if(q.isInside(l.mbr)) {
				ret = l.filename;
				break;
			}
		}
		return ret;
	}
	
	public static ArrayList<String> findRangeQueryPartition(Rect r,ArrayList<GlobalRecord> list){
		ArrayList<String> result = new ArrayList<String>();
		for(GlobalRecord l:list) {
			if(l.mbr.isIntersects(r)) {
				result.add(l.filename);
			}
		}
		return result;
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
	
	private static void deleteDir(String s)throws Exception{
		Configuration conf = new Configuration();
		Path output = new Path(s);
		FileSystem fs = output.getFileSystem(conf);
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
	}
}
