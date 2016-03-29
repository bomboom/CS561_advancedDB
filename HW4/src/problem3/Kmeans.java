package problem3;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Kmeans extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		//run args: <input file> <outputfile> <cache center> <new center file> <k center> <tolerence> <clusteroutput>
		//run args: input/data2.csv kmeans input/center.csv kmeans/part-r-00000 5 2 cluster
		
		int res = ToolRunner.run(new Configuration(), new Kmeans(), args);
		System.exit(res);
	}
	
	public static class classifyMapper 
			extends Mapper<Object, Text, Text, Text>{
		private static Log log = LogFactory.getLog(classifyMapper.class);
		private int k;
		private float[] center_x;
		private float[] center_y;
		
		@Override
		protected void setup(Context context
				)throws IOException, InterruptedException{
			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			this.k = Integer.parseInt(context.getConfiguration().get("kClass"));
			this.center_x = new float[this.k+1];
			this.center_y = new float[this.k+1];
			if(caches==null || caches.length<=0){
				log.error("no cache file");
				System.exit(1);
			}
			
			BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
			String line;
			while((line=br.readLine())!=null){
				String[] str = line.split("\t");
				int i = Integer.parseInt(str[0]);
				float x = Float.parseFloat(str[1]);
				float y = Float.parseFloat(str[2]);
				this.center_x[i] = x;
				this.center_y[i] = y;
			}
			br.close();
		}
		
		public void map(Object key, Text value, Context context
				)throws IOException, InterruptedException{
			
			String[] point = value.toString().split(",");
			float point_x = Float.parseFloat(point[1]);
			float point_y = Float.parseFloat(point[2]);
			float min = Float.MAX_VALUE;
			int cl = 0;
			for(int i=1;i<=k;i++){
				float dist = (point_x-center_x[i])*(point_x-center_x[i])+(point_y-center_y[i])*(point_y-center_y[i]);
				if(dist < min){
					min = dist;
					cl = i;
				}
			}
			Text cl_ = new Text();
			Text point_ = new Text();
			cl_.set(Integer.toString(cl));
			point_.set(point[1]+','+point[2]+",1");
			context.write(cl_, point_);
		
		}
		
	}//finish Kmeans-mapper
	
	public static class clusterMapper 
			extends Mapper<Object, Text, Text, Text>{
		private static Log log = LogFactory.getLog(classifyMapper.class);
		private int k;
		private float[] center_x;
		private float[] center_y;
		
		@Override
		protected void setup(Context context
				)throws IOException, InterruptedException{
			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			this.k = Integer.parseInt(context.getConfiguration().get("kClass"));
			this.center_x = new float[this.k+1];
			this.center_y = new float[this.k+1];
			if(caches==null || caches.length<=0){
				log.error("no cache file");
				System.exit(1);
			}
			
			BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
			String line;
			while((line=br.readLine())!=null){
				String[] str = line.split("\t");
				int i = Integer.parseInt(str[0]);
				float x = Float.parseFloat(str[1]);
				float y = Float.parseFloat(str[2]);
				this.center_x[i] = x;
				this.center_y[i] = y;
			}
			br.close();
		}
	
		public void map(Object key, Text value, Context context
				)throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String[] point = itr.nextToken().split(",");
				float point_x = Float.parseFloat(point[1]);
				float point_y = Float.parseFloat(point[2]);
				float min = Float.MAX_VALUE;
				int cl = 0;
				for(int i=1;i<=k;i++){
					float dist = (point_x-center_x[i])*(point_x-center_x[i])+(point_y-center_y[i])*(point_y-center_y[i]);
					if(dist < min){
						min = dist;
						cl = i;
					}
				}
				Text cl_ = new Text();
				Text point_ = new Text();
				cl_.set(Integer.toString(cl));
				point_.set(point[0]+", <"+point[1]+','+point[2]+'>');
				context.write(cl_, point_);
			}
		}
	
	}//finish cluster mapper, final result <cluster> <point>
	
	public static class centerCombiner extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> value, Context context
				)throws IOException, InterruptedException{
			float sum_x = 0;
			float sum_y = 0;
			int count = 0;
			for(Text val:value){
				String[] str = val.toString().split(",");
				sum_x += Float.parseFloat(str[0]);
				sum_y += Float.parseFloat(str[1]);
				count += Integer.parseInt(str[2]);
			}
			context.write(key, new Text(Float.toString(sum_x)+','+Float.toString(sum_y)+','+Integer.toString(count)));
		}
	}// finish combiner
	
	public static class centerReducer
			extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> value, Context context
				)throws IOException, InterruptedException{
			float sum_x = 0;
			float sum_y = 0;
			int count = 0;
			for(Text val:value){
				System.out.println(val.toString());
				String[] str = val.toString().split(",");
				sum_x += Float.parseFloat(str[0]);
				sum_y += Float.parseFloat(str[1]);
				count += Integer.parseInt(str[2]);
			}
			float avg_x = sum_x/count;
			float avg_y = sum_y/count;
			context.write(key, new Text(Float.toString(avg_x)+'\t'+Float.toString(avg_y)));
		}
		
	}//finish reducer
	
	@Override
	public int run(String[] arg0) throws Exception{
		//input:: <data> <output> <oldcenter> <newcenter> <k> <tolerance>
		int iteration = 0;
		
		do{
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, arg0).getRemainingArgs();
			if(otherArgs.length != 7){
				System.err.println("wrong ussage, run args: <input file> <outputfile> <cache(old) center> <new center file> <k center> <tolerence> <clusteroutput>");
				System.exit(2);
			}
			conf.set("oldcenters", otherArgs[2]);
			conf.set("kClass", otherArgs[4]);
			Job job = new Job(conf, "Kmeans");
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(classifyMapper.class);
			job.setCombinerClass(centerCombiner.class);
			job.setReducerClass(centerReducer.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(), job
	                .getConfiguration());
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(otherArgs[1]))){
				fs.delete(new Path(otherArgs[1]), true);
			}
			
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			job.waitForCompletion(true);
			
			iteration++;
			System.out.println("iteration:\n"+iteration);
		}while(iteration<6 && (Assistant.isFinished(arg0[2], arg0[3], Integer.parseInt(arg0[4]), Float.parseFloat(arg0[5]))==false));
		// final center point
		
		//get final cluster
		//<cluster> <point>
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, arg0).getRemainingArgs();
		if(otherArgs.length != 7){
			System.err.println("wrong ussage, run args: <input file> <outputfile> <cache(old) center> <new center file> <k center> <tolerence> <clusteroutput>");
			System.exit(2);
		}
		conf.set("oldcenters", otherArgs[2]);
		conf.set("kClass", otherArgs[4]);
		Job job = new Job(conf, "Kmeans");
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(clusterMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(), job
                .getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[6]));
		
		return job.waitForCompletion(true)? 0:1;
	}
	
	//Assistant help to justify if two iter center reach the threshold.
	public static class Assistant{
		// see if distance between old centers and new centers < threshold
		public static List<ArrayList<Float>> getCenters(String centerPath){
			List<ArrayList<Float>> centers = new ArrayList<ArrayList<Float>>();
			Configuration conf = new Configuration();
			
			try {
				FileSystem hdfs = FileSystem.get(conf);
				Path in = new Path(centerPath);
				FSDataInputStream fsin = hdfs.open(in);
				LineReader linein = new LineReader(fsin, conf);
				Text line = new Text();
				
				while(linein.readLine(line)>0){
					String record = line.toString();
					String[] fields = record.split("\t");
					List<Float> tmp = new ArrayList<Float>();
					for(int i=1;i<fields.length;i++){
						tmp.add(Float.parseFloat(fields[i]));
					}
					centers.add((ArrayList<Float>) tmp);
				}
				fsin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return centers;
		}
		
		public static void deleteLast(String path){
			Configuration conf = new Configuration();
			FileSystem hdfs;
			try {
				hdfs = FileSystem.get(conf);
				Path path1 = new Path(path);
				hdfs.delete(path1, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public static boolean isFinished(String oldPath, String newPath, int k, float threshold
				)throws IOException{
			List<ArrayList<Float>> oldCenters = getCenters(oldPath);
			List<ArrayList<Float>> newCenters = getCenters(newPath);
			
			float sum = 0;
			for(int i=0;i<k;i++){
				for(int j=0;j<oldCenters.get(i).size();j++){
					float dist = (oldCenters.get(i).get(j)-newCenters.get(i).get(j));
					sum += Math.pow(dist, 2);	
				}
			}
			
			System.out.println("distance:"+sum+" threshold:"+threshold);
			if(sum<threshold)	return true;
			else{
				//still need to iterate, replace oldPath with newPath
				deleteLast(oldPath);
				Configuration conf = new Configuration();
				FileSystem hdfs = FileSystem.get(conf);
				String path_ = "/home/hadoop/centertmp.csv";
				hdfs.copyToLocalFile(new Path(newPath), new Path(path_));
				hdfs.delete(new Path(oldPath), true);
				hdfs.moveFromLocalFile(new Path(path_), new Path(oldPath));
				
				return false;
			}
		}
	}//finish Assistant
}
