package MapReduceTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.conf.Configuration;

import quadIndex.Rect;
import debug.Debug;
import io.InputParser;
import STRTree.STRTreeWritable;


public class GlobalIndex {
	public static void run(String[] args)throws Exception {
		if(args.length<2) {
			System.out.println("args less than 2 <input> <output>");
			return;
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "GlobalIndex");
		job.setJarByClass(GlobalIndex.class);
		
		job.setMapperClass(GlobalIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(GlobalIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outPath = new Path(args[1]);
		//conf.set("fs.default.name", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)) {
        	fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
	}
}

class GlobalIndexMapper extends Mapper<Text,STRTreeWritable,Text,Text>{
	
	@Override
	public void map(Text treeid,STRTreeWritable strtree,Context context) throws IOException,InterruptedException{
		String line = strtree.root.MBR.toString()+" "+strtree.root.hashvalue;
		context.write(treeid, new Text(line));
	}
}

class GlobalIndexReducer extends Reducer<Text,Text,Text,Text>{
	@Override
	public void reduce(Text treeid,Iterable<Text> line,Context context)throws IOException,InterruptedException{
		for(Text t:line) {
			context.write(treeid, t);
		}
	}
}
