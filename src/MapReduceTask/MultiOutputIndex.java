package MapReduceTask;
import io.InputParser;

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
import STRTree.STRTreeWritable;


public class MultiOutputIndex {
	public static void run(String[] args)throws Exception{
		if(args.length<2) {
			System.out.println("args less than 2 <input> <output>");
			return;
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MultipleOutputIndex");
		job.setJarByClass(MultiOutputIndex.class);
		
		job.setMapperClass(MultiOutputIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Rect.class);
		
		job.setReducerClass(MultiOutputIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(STRTreeWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		conf.set("fs.default.name", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/hadoop/input");
        FileStatus[] listStatus = fs.listStatus(path);
        for(FileStatus fileStatus:listStatus) {
        	String p = fileStatus.getPath().toString();
        	String []split = p.split("/");
    		String name = split[split.length-1];
    		name = name.split(".txt")[0];
    		MultipleOutputs.addNamedOutput(job, name, SequenceFileOutputFormat.class,Text.class, STRTreeWritable.class);
        }
        Path outPath = new Path(args[1]);
        if(fs.exists(outPath)) {
        	fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        
	}

}

class MultiOutputIndexMapper extends Mapper<LongWritable,Text,Text,Rect>{
	
	@Override
	public void map(LongWritable no,Text content,Context context) throws IOException,InterruptedException{
		String line = content.toString();
		Rect rect = InputParser.getObjFromLine(line);
		InputSplit inputsplit = context.getInputSplit();
		String filename = ((FileSplit)inputsplit).getPath().toString();
		String []split = filename.split("/");
		String name = split[split.length-1];
		name = name.split(".txt")[0];
		Text tid = new Text(name);
		Debug.println("Tree id= "+tid.toString());
		context.write(tid, rect);
	}
	
}

class MultiOutputIndexReducer extends Reducer<Text,Rect,Text,STRTreeWritable>{
	private MultipleOutputs<Text,STRTreeWritable> multipleOutputs = null;
	static int count = 0;
	static int nodec = 0;
	@Override
	public void setup(Context context)throws IOException,InterruptedException {
		multipleOutputs = new MultipleOutputs<Text,STRTreeWritable>(context);
	}
	@Override
	public void reduce(Text key,Iterable<Rect> value,Context context)throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		nodec = conf.getInt("nodec", 3);
		ArrayList<Rect> rlist = new ArrayList<Rect>();
		for(Rect r:value) {
			Rect rn = new Rect(r.x1,r.x2,r.y1,r.y2);
			rlist.add(rn);
		}
		STRTreeWritable strtree = new STRTreeWritable(rlist,nodec);
		multipleOutputs.write(key.toString(), key, strtree);
	}
	
	@Override
	public void cleanup(Context context)throws IOException,InterruptedException{
		if(null != multipleOutputs) {
			multipleOutputs.close();
			multipleOutputs = null;
		}
	}
	
}





