package project1;

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Query3 extends Configured implements Tool {
	
	
	
	public static class JoinMapper
			extends Mapper<Object, Text, Text, Text>{
		//Save records for Customers relation
		private HashMap<String, String> customerMap = new HashMap<String, String>();
		private Text ID = new Text();
		private Text mapValue = new Text();
	
		//Read Customers.csv file into DistributedCache for map-side join
		@Override
		protected void setup(Context context
				) throws IOException, InterruptedException{
			BufferedReader bf = null;
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String line = "";
			
			for(Path path: paths){
				if(path.toString().contains("Customers")){
					bf = new BufferedReader(new FileReader(path.toString()));
					while((line=bf.readLine())!=null){
						// save to HashMap
						String[] str= line.split(",", 2);
						customerMap.put(str[0], str[1]);
					}
				}
			}
			
			bf.close();
		} // finish setup
		
		public void map(Object key, Text value, Context context
				)throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String[] recordT = itr.nextToken().toString().split(",");
				String[] recordC = customerMap.get(recordT[1]).split(",");
				// ID name salary
				ID.set(recordT[1]+" "+recordC[0]+" "+recordC[3]);
				//  #transaction transNum transItem
				mapValue.set("1 "+recordT[2]+" "+recordT[3]);
				context.write(ID, mapValue);
			}
		}
	
	}// finish mapper
	
	public static class sumReducer 
			extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> value, Context context
				) throws IOException, InterruptedException{
			int sum = 0;	//#transacion
			float total = 0;		//total money of transactions
			int min = 11;	//min transItem
			for(Text val:value){
				String[] rec = val.toString().split(" ");
				sum += Integer.parseInt(rec[0]);
				total += Float.parseFloat(rec[1]);
				min = min>Integer.parseInt(rec[2])? Integer.parseInt(rec[2]):min;
			}
			result.set(Integer.toString(sum)+" "+Float.toString(total)+" "+Integer.toString(min));
			context.write(key, result);
		}
	}// finish reducer

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Query3");
		job.setJarByClass(Query3.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(sumReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		DistributedCache.addCacheFile(new Path(arg0[0]).toUri(), job
                .getConfiguration());
		FileInputFormat.addInputPath(job, new Path(arg0[1]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new Query3(),
                args);
        System.exit(res);
	}
}
