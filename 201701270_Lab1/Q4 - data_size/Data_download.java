package downloadData;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class downloadData 
{
	public static class downloadMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
		{
			int i=0,fsz=0;
			String ss="";
			String s = value.toString();
			while(i<s.length() && s.charAt(i)!='[')
			{
				i++;
			}
			i+=4;
			String month=String.valueOf(s.charAt(i))+String.valueOf(s.charAt(i+1))+String.valueOf(s.charAt(i+2));
			i+=4;
			String year=String.valueOf(s.charAt(i))+String.valueOf(s.charAt(i+1))+String.valueOf(s.charAt(i+2))+String.valueOf(s.charAt(i+3));
			value.set(month+"-"+year);
			while(i<s.length() && s.charAt(i)!='"')
			{
				i++;
			}
			i++;
			while(i<s.length() && s.charAt(i)!='"')
			{
				i++;
			}
			i++;
			while(i<s.length() && s.charAt(i)!=' ')
			{
				i++;
			}
			i+=2;
			while(i<s.length() && s.charAt(i)!=' ')
			{
				i++;
			}
			i++;
			if(s.charAt(i)!='-')
			{
				while(i<s.length() && s.charAt(i)!=' ')
				{
					ss+=s.charAt(i);
					i++;
				}
				fsz=Integer.parseInt(ss);
			}
			
			context.write(value ,new IntWritable(fsz));
		}
	}
	
	public static class downloadReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
		{
			int adder=0,counter=0;
			for(IntWritable x: values)
			{
				adder+=x.get();
				counter++;
			}
			String c_s=Integer.toString(counter);
			String key_n = key.toString();
			key.set(key_n+" "+c_s);
			context.write(key, new IntWritable(adder));
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf,"dataSize");
		job.setJarByClass(downloadData.class);
		
		job.setMapperClass(downloadMapper.class);
		job.setReducerClass(downloadReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath,true);
		//exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
