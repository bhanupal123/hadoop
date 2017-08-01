package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MyPartitioner implements Partitioner<Text,IntWritable>{

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPartition(Text arg0, IntWritable arg1, int arg2) {
		// TODO Auto-generated method stub
		String s=arg0.toString();
		if(s.length()==1)
		{return 0;}
		else if(s.length()==2)
		{return 1;}
		else
		{return 2;}
		
		
	}

}
