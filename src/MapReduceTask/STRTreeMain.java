package MapReduceTask;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

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
		//String [] rangequery = {"1,1,3,7,16","tree","output"};
		String[] knn = {"1,1,7,4","tree","output"};
		//STRTreeIndex.run(index);
		//STRTreeRangeQuery.run(rangequery);
		STRTreeKNN.run(knn);
		String knnresult = getkNN("hdfs://localhost:9000/user/hadoop/output/part-00000");
		int check = knnresult.indexOf("needrange");
		//System.out.println("check" +check);
		if(check != -1) {
		String[] mbr = knnresult.split("\t")[1].split(" ");
		String knnrange = "2,"+mbr[1]+","+mbr[2]+","+mbr[3]+","+mbr[4];
		try {
			deleteDir("output");
		}catch(Exception e) {}
		String [] knnrangequery = {knnrange,"tree","output"};
		STRTreeRangeQuery.run(knnrangequery);
		
		}
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
