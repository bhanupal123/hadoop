package Aadhar;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class aadhar_by_eachState  {
	static class map1 extends Mapper<LongWritable,Text,Text,IntWritable>
	{	
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String[] a=value.toString().split(",");
			context.write(new Text(a[2]), new IntWritable(Integer.parseInt(a[8])));
		}
		
	}
	static class red1 extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void red(Text key,Iterable<IntWritable>value,Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable count:value)
			{
				sum+=count.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
	static class  sortmap extends Mapper<LongWritable,Text,IntWritable,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String[] a=value.toString().split(",");
			context.write(new IntWritable(Integer.parseInt(a[1].trim())),new Text(a[0].trim()));
		}
	}
	
	static class finalred extends Reducer<IntWritable,Text,Text,IntWritable>
	{
		
		public void red(IntWritable key,Iterable<Text>value,Context context) throws IOException, InterruptedException
		{
			for(Text test: value)
			{
				context.write(test, key);
			}
		}
		
		
	}
	
	static class SortComparator extends WritableComparator {
	public int compare(WritableComparable k1,WritableComparable k2)
	{
		IntWritable v1 = (IntWritable) k1;
		IntWritable v2 = (IntWritable) k2;
		return v1.get() < v2.get() ? 1 : v1.get() == v2.get() ? 0 : -1; 
	}
	protected SortComparator() {
        super(IntWritable.class, true);
    } 
	
	}
	
	
	
	public static void main(String[] args) throws Exception  {

		if (args.length != 3) {
			System.out.println("Usage: [input] [output1] [output2]");
			System.exit(-1);
		}
		Configuration conf=new Configuration();
		Job stateWiseCount = Job.getInstance(conf,"Aadhaar Data Analysis");
		
		stateWiseCount.setJarByClass(aadhar_by_eachState.class);

		stateWiseCount.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
		
		stateWiseCount.setMapperClass(map1.class);
		stateWiseCount.setReducerClass(red1.class);
		stateWiseCount.setMapOutputKeyClass(Text.class);
		stateWiseCount.setMapOutputValueClass(IntWritable.class);

		stateWiseCount.setOutputKeyClass(Text.class);
		stateWiseCount.setOutputValueClass(IntWritable.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(stateWiseCount, inputFilePath);
		FileOutputFormat.setOutputPath(stateWiseCount, outputFilePath);

		int code = stateWiseCount.waitForCompletion(true) ? 0 : 1;
		
		if(code == 0)
		{
		Configuration conf1=new Configuration();
		Job sort = Job.getInstance(conf1,"Sorting States on Num Aadhaars generated");
		
		sort.setJarByClass(aadhar_by_eachState.class);		
		
		sort.setOutputKeyClass(Text.class);
		sort.setOutputValueClass(IntWritable.class);

		sort.setMapperClass(sortmap.class);
		sort.setReducerClass(finalred.class);
		sort.setSortComparatorClass(SortComparator.class);
		
		sort.setMapOutputKeyClass(IntWritable.class);
		sort.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(sort, new Path(args[1]));
		FileOutputFormat.setOutputPath(sort, new Path(args[2]));

	
		
		System.exit(sort.waitForCompletion(true)?0:1);
		
		}
	}

}
	


