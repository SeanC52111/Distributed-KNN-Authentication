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

public class STRTreeMain {
	public static void main(String []args)throws IOException {
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
		String[] knn = {"1,2,4,2","tree","output"};
		//STRTreeIndex.run(index);
		//STRTreeRangeQuery.run(rangequery);
		STRTreeKNN.run(knn);
		/*
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/user/hadoop/output/part-00000"),conf);
		InputStream in=null;
		try {
			in = fs.open(new Path("hdfs://localhost:9000/user/hadoop/output/part-00000"));
			BufferedReader  br = new BufferedReader(new InputStreamReader(in));
			while(br.ready()) {
				String line = br.readLine();
				System.out.println(line);
			}
			br.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			
		}
		*/
	}
	
}
