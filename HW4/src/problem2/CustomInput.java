package problem2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import problem1.SpacialJoin.outReducer;
import problem1.SpacialJoin.regMapper;

public class CustomInput {
	public static class CustomerRecordReader extends RecordReader<LongWritable, Text>{
		
		private long start;
	    private long pos;
	    private long end;
	    private LongWritable key = new LongWritable();
	    private Text value = new Text();
	    private int maxLineLength;
	 
	    private final Log LOG = LogFactory.getLog(
	            CustomerRecordReader.class);
	    
		private LineReader in;
	    //private final static Text EOL = new Text("\n");
	    private String DELIMITER = "},";
	    private int maxLengthRecord;
	    
	    @Override
	    public void initialize(InputSplit Gsplit, TaskAttemptContext context
	    		)throws IOException, InterruptedException{
	    	Configuration job = context.getConfiguration();
	    	
	    	FileSplit split = (FileSplit) Gsplit;
	    	 
	        // Retrieve configuration, and Max allowed
	        // bytes for a single record
	        this.maxLineLength = job.getInt(
	                "mapred.linerecordreader.maxlength",
	                Integer.MAX_VALUE);
	 
	        // Split "S" is responsible for all records
	        // starting from "start" and "end" positions
	        start = split.getStart();
	        end = start + split.getLength();
	 
	        // Retrieve file containing Split "S"
	        final Path file = split.getPath();
	        FileSystem fs = file.getFileSystem(job);
	        FSDataInputStream fileIn = fs.open(split.getPath());
	 
	        // If Split "S" starts at byte 0, first line will be processed
	        // If Split "S" does not start at byte 0, first line has been already
	        // processed by "S-1" and therefore needs to be silently ignored
	        boolean skipFirstLine = false;
	        if (start != 0) {
	            skipFirstLine = true;
	            // Set the file pointer at "start - 1" position.
	            // This is to make sure we won't miss any line
	            // It could happen if "start" is located on a EOL
	            --start;
	            fileIn.seek(start);
	        }
	 
	        in = new LineReader(fileIn, job);
	 
	        // If first line needs to be skipped, read first line
	        // and stores its content to a dummy Text
	        if (skipFirstLine) {
	            Text dummy = new Text();
	            // Reset "start" to "start + line offset"
	            start += in.readLine(dummy, 0,
	                    (int) Math.min(
	                            (long) Integer.MAX_VALUE, 
	                            end - start));
	        }
	 
	        // Position is the actual start
	        this.pos = start;
	    }
	    
	    private int readNext(Text text, int maxLineLength, int maxBytesToConsume
	    		)throws IOException{
	    	int offset = 0;
	    	text.clear();
	    	Text tmp = new Text();
	    	
	    	for(int i=0;i<maxBytesToConsume;i++){
	    		int offsetTmp = in.readLine(tmp, maxLineLength, maxBytesToConsume);
	    		offset += offsetTmp;
	    		if(offsetTmp==0)	break;
	    		if(DELIMITER.equals(tmp.toString()))	break;
	    		else{
	    			//append value to record
	    			String line = tmp.toString();
	    			String[] apart = line.split(":");
	    			Text k = new Text(apart[1]);
	    			text.append(k.getBytes(), 0, k.getLength());
	    		}
	    	}
	    	
	    	return offset;
	    }

		@Override
		public void close() throws IOException {
			if (in != null) {
	            in.close();
	        }
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (start == end) {
	            return 0.0f;
	        } else {
	            return Math.min(1.0f, (pos - start) / (float) (end - start));
	        }
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			key.set(pos);
			 
	        int newSize = 0;
	 
	        // Make sure we get at least one record that starts in this Split
	        while (pos < end) {
	            newSize = readNext(value, maxLineLength,
	                    Math.max((int) Math.min(
	                            Integer.MAX_VALUE, end - pos),
	                            maxLineLength));
	 
	            // No byte read, seems that we reached end of Split
	            // Break and return false (no key / value)
	            if (newSize == 0) {
	                break;
	            }
	 
	            // Line is read, new position is set
	            pos += newSize;
	 
	            if (newSize < maxLineLength) {
	                break;
	            }
	 
	            LOG.info("Skipped line of size " + 
	                    newSize + " at pos "
	                    + (pos - newSize));
	        }
	 
	         
	        if (newSize == 0) {
	            // We've reached end of Split
	            key = null;
	            value = null;
	            return false;
	        } else {
	            // Tell Hadoop a new line has been found
	            // key / value will be retrieved by
	            // getCurrentKey getCurrentValue methods
	            return true;
	        }
		}
	    
	}
	
	public static class CustomerInputFormat 
			extends FileInputFormat<LongWritable, Text>{
		
		@Override
		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context
				)throws IOException,InterruptedException{
			return new CustomerRecordReader();
		}
		
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
	        return false;
	     }
		
	}// finish myinputformat
	
	public static class RecordMapper extends
    		Mapper<LongWritable, Text, Text, Text> {

		private Text out = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] apart = value.toString().split(",");
			out.set(apart[3]);
			if(apart[4].equals("female")){
				context.write(out, new Text("f:1"));
			}else{
				context.write(out, new Text("m:1"));
			}
		}
	}// finish mapper
	
	public static class OutReducer extends
			Reducer<Text, Text, Text, Text>{
		private Text out = new Text();
		public void reduce(Text key, Iterable<Text> value, Context context
				)throws IOException, InterruptedException{
			int f_count = 0;
			int m_count = 0;
			for(Text val:value){
				String[] apart = val.toString().split(":");
				if(apart[0].equals("f"))	f_count++;
				else	m_count++;
			}
			out.set("f:"+Integer.toString(f_count)+", m:"+Integer.toString(m_count));
			context.write(key, out);
		}
	}// finish reducer
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		if(args.length!=2){
			System.err.println("Wrong ussage");
			System.exit(2);
		}
		
		Job job = new Job(conf, "CustomerInput");
		job.setJarByClass(CustomInput.class);
		job.setMapperClass(RecordMapper.class);
		job.setReducerClass(OutReducer.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(CustomerInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0:1);
	}
}
