package MapReduceTask;
import java.io.*;
import java.io.InputStream;
import java.io.BufferedReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class FileRead {
	public static void main(String []args) throws Exception{
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
		}
		finally {
			
		}
		
	}
}
