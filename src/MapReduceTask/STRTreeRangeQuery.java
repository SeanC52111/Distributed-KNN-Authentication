package MapReduceTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;

import STRTree.*;
import debug.Debug;
import quadIndex.*;
import Tool.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;


public class STRTreeRangeQuery {
	
	public static void run(String[] args) {
		String tree_path = null;
		String out_path = null;
		double x1 = Double.MIN_VALUE;
		double x2 = Double.MAX_VALUE;
		double y1 = Double.MIN_VALUE;
		double y2 = Double.MAX_VALUE;
		long rqid = 0;
		if(args.length < 1)
		{
			System.out.println("Please specify query range.");
			return;
		}
		else {
			String [] rq = args[0].split(",");
			if(rq.length < 5) {
				System.out.println("Invalid range query syntax.");
				return ;
			}
			
			rqid = Long.parseLong(rq[0]);
			x1 = Double.parseDouble(rq[1]);
			x2 = Double.parseDouble(rq[2]);
			y1 = Double.parseDouble(rq[3]);
			y2 = Double.parseDouble(rq[4]);
			
			if(args.length < 3) {
				tree_path = "tree";
				out_path = "output";
			}else {
				tree_path = args[1];
				out_path = args[2];
			}
		}
		
		
		JobConf conf = new JobConf(STRTreeRangeQuery.class);
		
		conf.setJobName("range_query");
		conf.setLong("rq_id", rqid);
		conf.setDouble("x1", x1);
		conf.setDouble("x2", x2);
		conf.setDouble("y1", y1);
		conf.setDouble("y2", y2);
		
		conf.setMapperClass(STRTreeRangeQueryMapper.class);
		conf.setReducerClass(STRTreeRangeQueryReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(tree_path));
		FileOutputFormat.setOutputPath(conf, new Path(out_path));
		
		try {
			JobClient.runJob(conf);
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	
}

class STRTreeRangeQueryMapper extends MapReduceBase
	implements Mapper<Text,STRTreeWritable,Text,Text>{
	long rq_id = 0;
	Rect range = null;
	
	@Override
	public void configure(JobConf job) {
		rq_id = job.getLong("rq_id", 0);
		double x1 = job.getDouble("x1", Double.MAX_VALUE);
		double x2 = job.getDouble("x2", Double.MAX_VALUE);
		double y1 = job.getDouble("y1", Double.MAX_VALUE);
		double y2 = job.getDouble("y2", Double.MAX_VALUE);
		
		range = new Rect(x1,x2,y1,y2);
	}
	
	@Override
	public void map(Text treeid,STRTreeWritable strtree,OutputCollector<Text,Text> oc, Reporter rpt)
			throws IOException{
		Debug.println("Treeid = "+treeid.toString());
		LongWritable lw = new LongWritable(rq_id);
		
		LinkedList<String> VO = new LinkedList<String>();
		ArrayList<Rect> result = new ArrayList<Rect>();
		//System.out.println("root mbr:"+strtree.root.MBR.toString());
		//strtree.DFStraverse();
		strtree.secureRangeQuery(strtree.root, range, result,VO);
		Debug.println("Find "+result.size()+" lakes.");
		for(Rect r : result) {
			Debug.println(r.toString());
			oc.collect(new Text(lw.toString()), new Text(r.toString()));
		}
		String vos = "";
		for(String s : VO) {
			vos += s + "#";
		}
		oc.collect(new Text("VO"), new Text(vos));
		oc.collect(new Text("root_sig"), new Text(strtree.root.MBR.toString()+strtree.root.hashvalue));
	}
}

class STRTreeRangeQueryReducer extends MapReduceBase
	implements Reducer<Text,Text,Text,Text> {
	
	@Override
	public void reduce(Text rqid,Iterator<Text> rect_it, OutputCollector<Text,Text>oc, Reporter rpt)
		throws IOException{
		
		int total = 0;
		if(rqid.toString().equals("VO")) {
			while(rect_it.hasNext()) {
				Text vos = new Text(rect_it.next());
				oc.collect(rqid, vos);
				Debug.println("VOs: "+vos.toString());
			}
			
		}else if(rqid.toString().equals("root_sig")) {
			while(rect_it.hasNext()) {
				Text rootsig = new Text(rect_it.next());
				oc.collect(rqid, rootsig);
				Debug.println("root_sig: "+rootsig.toString());
			}
			
		}
		else {
			while(rect_it.hasNext()) {
				Text r = rect_it.next();
				oc.collect(rqid, new Text(r.toString()));
				total++;
			}
			
			oc.collect(rqid, new Text("Summary: ["+total+"] lakes are found."));
			Debug.println("Summary: ["+total+"] lakes are found.");
		}
		
	}
}













