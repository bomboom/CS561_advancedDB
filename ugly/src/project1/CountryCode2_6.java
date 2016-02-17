package project1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountryCode2_6 {

	public static class TokenizerMapper 
			extends Mapper<Object, Text, Text, Text>{
    
		private Text customer = new Text();
      
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	    	String records = itr.nextToken();
	    	String[] sep = records.split(",");
	    	if(Integer.parseInt(sep[3])>=2 && Integer.parseInt(sep[3])<=6){
		        customer.set(sep[0]);
		        //System.out.println(customer);
		        Text h = new Text(sep[3]);
		        context.write(customer,h);
	    	}
	      }
	    }
  } //mapper finish
 
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
	      System.err.println("wrong ussage");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Query1");
	    job.setJarByClass(CountryCode2_6.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
