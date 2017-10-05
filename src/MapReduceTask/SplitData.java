package MapReduceTask;
import io.InputParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapred.FileSplit;

import quadIndex.Rect;
import debug.Debug;
import STRTree.STRTreeWritable;


public class SplitData {
	public static void run(String []args) {
		String in_path = null;
		String out_path = null;
		if(args.length<2) {
			in_path = "input";
			out_path = "tree";
		}
		else {
			in_path = args[0];
			out_path = args[1];
		}
		JobConf conf = new JobConf(STRTreeIndex.class);
		conf.setJobName("index");
		conf.setMapperClass(SplitDataMapper.class);
		conf.setReducerClass(SplitDataReducer.class);
		
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Rect.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(STRTreeWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(in_path));
		FileOutputFormat.setOutputPath(conf, new Path(out_path));
		
		try {
			JobClient.runJob(conf);
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}


//input (key,value) = lineno, line
//output (key value) = count to number of calls of map, Rect
class SplitDataMapper extends MapReduceBase
	implements Mapper<LongWritable,Text,DoubleWritable,Text>{
	static int inst_count = 0;
	static enum IndexCounters {SIZE_E,COUNT_E};
	
	final static int sizeof_boolean = Integer.SIZE/8;
	final static int sizeof_int = Integer.SIZE/8;
	final static int sizeof_double = Double.SIZE/8;
	final static int sizeof_hash = 64;
	final static int max_block_size = 127*1024*1024;
	
	synchronized LongWritable getTreeId(Text content,Reporter rpt) {
		//String path = ((FileSplit)rpt.getInputSplit()).getPath().toString();
		Counters.Counter size_cc = rpt.getCounter(IndexCounters.SIZE_E);
		Counters.Counter count_cc = rpt.getCounter(IndexCounters.COUNT_E);
		//size of data plus tree node
		int dsize = sizeof_double*4 + sizeof_boolean*2+sizeof_int+sizeof_hash;
		if(dsize + size_cc.getValue()+sizeof_int>max_block_size) {
			size_cc.setValue(dsize);
			count_cc.increment(1);
			return new LongWritable(count_cc.getValue());
		}
		else
		{
			size_cc.increment(dsize);
			return new LongWritable(count_cc.getValue());
		}
	}
	
	@Override
	public void map(LongWritable key,Text value,OutputCollector<DoubleWritable,Text>oc,Reporter rpt) throws IOException{
		String line = value.toString();
		String []split = line.split(" ");
		DoubleWritable d = new DoubleWritable(Double.valueOf(split[0]));
		oc.collect(d,value);
	}
}

//input (key,value) = count in each mapper, Rect
//output (key,value) = _, STRTree

class SplitDataReducer extends MapReduceBase
	implements Reducer<LongWritable,Rect,Text,STRTreeWritable>{
	static int count = 0;
	static int nodec = 0;
	
	@Override
	public void configure(JobConf job) {
		nodec = job.getInt("nodec", 3);
	}
	
	@Override
	public void reduce(LongWritable key,Iterator<Rect> value,OutputCollector<Text,STRTreeWritable> oc,Reporter rpt)throws IOException{
		count++;
		ArrayList<Rect> rlist = new ArrayList<Rect>();
		while(value.hasNext()) {
			Rect r = new Rect();
			r = value.next();
			Rect rn = new Rect(r.x1,r.x2,r.y1,r.y2);
			rlist.add(rn);
		}
		//Debug.println("before writing tree");
		STRTreeWritable strtree = new STRTreeWritable(rlist,3);
		//strtree.DFStraverse();
		//Debug.println("after writing tree");
		Text k = new Text(String.valueOf(key));
		oc.collect(k, strtree);
	}
}

