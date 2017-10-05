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


public class STRTreeKNN {
	public static void run(String[]args) {
		String tree_path = null;
		String out_path = null;
		long rqid = 0;
		double x=0;
		double y=0;
		int k=1;
		if(args.length<1) {
			System.out.println("please specify knn.");
			return;
		}
		else {
			String[] rq = args[0].split(",");
			if(rq.length < 4) {
				System.out.println("Invalid knn syntax");
				return;
			}
			rqid = Long.parseLong(rq[0]);
			k = Integer.parseInt(rq[1]);
			x = Double.parseDouble(rq[2]);
			y = Double.parseDouble(rq[3]);
			
			if(args.length < 3) {
				tree_path = "tree";
				out_path = "output";
			}else {
				tree_path = args[1];
				out_path = args[2];
			}
		}
		
		JobConf conf = new JobConf(STRTreeKNN.class);
		
		conf.setJobName("knn");
		conf.setLong("rq_id", rqid);
		conf.setInt("k", k);
		conf.setDouble("x", x);
		conf.setDouble("y", y);
		
		conf.setMapperClass(STRTreeKNNMapper.class);
		conf.setReducerClass(STRTreeKNNReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(tree_path));
		FileOutputFormat.setOutputPath(conf, new Path(out_path));
		
		try {
			JobClient.runJob(conf);
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

}


class STRTreeKNNMapper extends MapReduceBase
	implements Mapper<Text,STRTreeWritable,Text,Text>{
	long rq_id = 0;
	Point q = null;
	int k = 0;
	
	@Override
	public void configure(JobConf job) {
		rq_id = job.getLong("rq_id", 0);
		double x = job.getDouble("x", 0);
		double y = job.getDouble("y", 0);
		k = job.getInt("k", 1);
		q = new Point(x,y);
	}
	
	@Override
	public void map(Text treeid,STRTreeWritable strtree,OutputCollector<Text,Text> oc,Reporter rpt)
		throws IOException{
		Debug.println("Treeid ="+treeid.toString());
		LongWritable lw = new LongWritable(rq_id);
		LinkedList VO = new LinkedList();
		LinkedList<String> result = new LinkedList<String>();
		//System.out.println("root mbr: "+strtree.root.MBR.toString());
		//boolean isinside = q.isInside(strtree.root.MBR);
		//System.out.println(isinside == true);
		//query q point is inside the partial tree
		if(q.isInside(strtree.root.MBR)) {
			
			strtree.secureKNN(k, q, result, VO);
			Debug.println("Find "+result.size()+" lakes.");
			
			for(String r:result) {
				Debug.println(r);
				oc.collect(new Text(lw.toString()), new Text(r));
			}
			String vos = "";
			for(Object s: VO) {
				if(s instanceof String)
					vos += s + "#";
			}
			oc.collect(new Text("VO"), new Text(vos));
			oc.collect(new Text("root_sig"), new Text(strtree.root.MBR.toString()+strtree.root.hashvalue));
			
		}
		else {
			VO.add("[");
			VO.add("("+strtree.root.MBR.toString()+" "+strtree.root.hashvalue+")");
			VO.add("]");
			String vos = "";
			for(Object s : VO) {
				if(s instanceof String)
					vos += s + "#";
			}
			oc.collect(new Text("VO"), new Text(vos));
			oc.collect(new Text("root_sig"), new Text(strtree.root.MBR.toString()+strtree.root.hashvalue));
		}
		
	}
}



class STRTreeKNNReducer extends MapReduceBase
	implements Reducer<Text,Text,Text,Text> {
	
	@Override
	public void reduce(Text rqid,Iterator<Text> rect_it,OutputCollector<Text,Text>oc, Reporter rpt)
		throws IOException{
		int total = 0;
		if(rqid.toString().equals("VO")) {
			while(rect_it.hasNext()) {
				Text vos = new Text(rect_it.next());
				oc.collect(rqid, vos);
				Debug.println("VOs "+vos.toString());
			}
		}
		else if(rqid.toString().equals("root_sig")) {
			while(rect_it.hasNext()) {
				Text rootsig = new Text(rect_it.next());
				oc.collect(rqid, rootsig);
				Debug.println("root_sig "+rootsig.toString());
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