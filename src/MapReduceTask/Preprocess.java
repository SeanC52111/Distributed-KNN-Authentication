package MapReduceTask;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class SortMapper
	extends Mapper<LongWritable,Text,DoubleWritable,Text>{
	
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		String line = value.toString();
		String []split = line.split(" ");
		DoubleWritable d = new DoubleWritable(Double.valueOf(split[0]));
		context.write(d, value);
	}
}

class SortReducer
	extends Reducer<DoubleWritable,Text,Text,NullWritable>{
	@Override
	public void reduce(DoubleWritable key,Iterable<Text> values,Context context) 
	throws IOException,InterruptedException{
		for(Text value:values) {
			context.write(value, NullWritable.get());
		}
	}
}

public class Preprocess{
	public static void main(String[] args)throws Exception{
		if(args.length != 2)
		{
			System.err.println("Usage: <input path><output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(Preprocess.class);
		job.setJobName("Sort");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
