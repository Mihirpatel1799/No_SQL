package maxTemp;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class maxTemperature
{
	public static class maxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private static final int MAXI = 9999;
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String parser = value.toString();
			String year = parser.substring(15, 19);
			int temp_air;
			if (parser.charAt(87) == '+') 
			{ 
				temp_air = Integer.parseInt(parser.substring(88, 92));
			} 
			else 
			{
				temp_air = Integer.parseInt(parser.substring(87, 92));
			}
			String qual = parser.substring(92, 93);
			if (temp_air != MAXI && qual.matches("[01459]")) 
			{
				context.write(new Text(year), new IntWritable(temp_air));
			}
		}
	}
	
	public static class maxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
	  
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException 
	  {
	    
	    int MAXI_VAL = Integer.MIN_VALUE;
	    for (IntWritable value : values) 
	    {
	      MAXI_VAL = Math.max(MAXI_VAL, value.get());
	    }
	    context.write(key, new IntWritable(MAXI_VAL));
	  }
	}

	public static void main(String[] args) throws Exception
	{
	    if (args.length != 2) 
	    {
	      System.err.println("Usage: MaxTemperature <input path> <output path>");
	      System.exit(-1);
	    }
	    
	    Configuration conf= new Configuration();
	    Job job = Job.getInstance(conf,"maxTemp");
	    job.setJarByClass(maxTemperature.class);
	    
	    job.setMapperClass(maxTemperatureMapper.class);
	    job.setReducerClass(maxTemperatureReducer.class);
	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	}

}