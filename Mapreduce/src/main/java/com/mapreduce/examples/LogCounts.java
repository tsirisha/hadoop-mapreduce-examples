package com.mapreduce.examples;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.Date;

/**
 * Run code
 *  hadoop jar logcounts.jar com.mapreduce.examples.LogCounts logfile.log mapperlog1

 * @author sirisha
 *
 */
public class LogCounts {
	public static class LogMap extends MapReduceBase implements Mapper<LongWritable, Text, DateErrorWritable, IntWritable>
	{
		private DateErrorWritable theDate = new DateErrorWritable();
		private final static IntWritable one = new IntWritable( 1 );
		Date theDateObj=new Date();
		private final static SimpleDateFormat formatter = new SimpleDateFormat(  "yyyy-MM-dd' 'HH:mm:ss" );
		public void map( LongWritable key, // Offset into the file
				Text value,
				OutputCollector<DateErrorWritable, IntWritable> output,//store intermediate values
				Reporter reporter) throws IOException
		{
			String line = value.toString();
			int count=0;

			StringTokenizer tokenizer = new StringTokenizer(line,",");
			while(tokenizer.hasMoreTokens()){
				
				
				if(count==0){
					String dateString=tokenizer.nextToken();
					//theDate.setDate(formatter.parse(myDate));

					// Build a date object from a string of the form: yyyy-MM-dd' 'HH:mm:ss
					int index = 0;
					int nextIndex = dateString.indexOf( '-' );
					int year = Integer.parseInt(dateString.substring(index, nextIndex));

					index = nextIndex;
					nextIndex = dateString.indexOf( '-', index+1 );
					int month = Integer.parseInt(dateString.substring( index+1, nextIndex ));

					index = nextIndex;
					nextIndex = dateString.indexOf( ' ', index+1 );
					int day = Integer.parseInt( dateString.substring(index+1, nextIndex) );

					// Build a calendar object for this date
					Calendar calendar = Calendar.getInstance();
					calendar.set( Calendar.DATE, day );
					calendar.set( Calendar.YEAR, year );
					calendar.set( Calendar.MONTH, month);
					calendar.set( Calendar.HOUR, 0 );
					calendar.set( Calendar.MINUTE, 0 );
					calendar.set( Calendar.SECOND, 0 );
					calendar.set( Calendar.MILLISECOND, 0 );


					// Output the date as the key and 1 as the value
					theDate.setDate( calendar.getTime() );


					//output.collect(theDate, one);
				}
				count++;
				String myString=tokenizer.nextToken();
				
				//theDate.setTheError(myString);
				if(myString.indexOf("ERROR")>=0)
					theDate.setTheError("ERROR");
				else{
				if(myString.indexOf("WARN")>=0)
					theDate.setTheError("WARN");
				else{
				if(myString.indexOf("DEBUG")>=0)
					theDate.setTheError("DEBUG");
				else{
				if(myString.indexOf("FATAL")>=0)
					theDate.setTheError("FATAL");
				else{
				if(myString.indexOf("INFO")>=0)
					theDate.setTheError("INFO");
				}
				}
				}
				}
				
			}output.collect(theDate, one);
		}
	}
	public static class LogReduce extends MapReduceBase
	implements Reducer<DateErrorWritable, IntWritable, DateErrorWritable, IntWritable> {

		public void reduce(DateErrorWritable key, Iterator<IntWritable> values,
				OutputCollector<DateErrorWritable, IntWritable> output,
				Reporter reporter)
						throws IOException {
			int count = 0;
			while( values.hasNext() )
			{
				count += values.next().get();
			}
			// Output the word with its count (wrapped in an IntWritable)
			output.collect( key, new IntWritable( count ) );

		}


	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration config = new Configuration();
		config.set("myfile", args[0]);


		FileSystem fs= FileSystem.get(config);
		Path filenamepath = new Path(args[0]);
		config.set("key.value.separator.in.input.line", ",");  // set input key-value separator 

		JobConf  conf = new JobConf(config);
		conf.setJarByClass(LogCounts.class);
		conf.setJobName("logcounts");

		conf.setOutputFormat( TextOutputFormat.class );
		conf.setOutputKeyClass( DateErrorWritable.class );
		conf.setOutputValueClass( IntWritable.class );

		conf.setMapperClass(LogMap.class);
		conf.setCombinerClass(LogReduce.class);
		conf.setReducerClass(LogReduce.class);





		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		//System.exit(conf.waitForCompletion(true) ? 0 : 1);
	}
}	
