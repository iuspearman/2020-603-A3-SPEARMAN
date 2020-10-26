package Hadoop.MapReduce;

import java.io.*;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;

import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader.ArffReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRkNN {
	public static class IdxMapper extends Mapper<Object, Text, IntWritable, IntWritable>{		
		private Instances data;
		private int k = 3;
		private IntWritable count = new IntWritable(0);

		
		
		private void parse_arff(){
			try {
			BufferedReader reader = new BufferedReader(new FileReader("small.arff")) ; 
			ArffReader arff = new ArffReader(reader);
			data = arff.getData();
			data.setClassIndex(data.numAttributes() - 1);
			}catch(IOException ioe) {
				System.err.println("error reading arff");
			}
		}
			
		private int calc_dist(String row) {
			String[] atts = row.split(",");
			float[] dists = new float[k];
			int[] neighs = new int[k];
			int[] sdc_arr = new int[k];
			int max_idx = 0;
			int min_idx = 0;
			int sdc = 0;
			for(int i = 0; i < k; i++) {
				dists[i] = Float.MAX_VALUE;
			}			
			
			for(int i = 0; i < data.numInstances(); i++) {	
				double distance = 0;
				for(int j = 0; j < data.numAttributes()-1; j++) {
					float diff = Float.parseFloat(atts[j]) - (float) data.instance(i).value(j);
					distance += diff * diff;
				}
				distance = Math.sqrt(distance);
				for(int j = 0; j < k; j++) {
					if(dists[j] > dists[max_idx]) {
						max_idx = j;
					}
				}
				if(distance < dists[max_idx]) {
					dists[max_idx] = (float) distance;
					neighs[max_idx] = i;
					sdc_arr[max_idx] = (int)data.instance(i).value(data.numAttributes()-1);
				}
			}
			
			int max_count = 0;
			
			
			for(int i = 0; i < k; i++) {
				int count = 0;
				for(int j = 0; j < k; j++) {
					if(sdc_arr[i] == sdc_arr[j]) {
						count++;
					}
				}
				if(count>max_count) {
					max_count = count;
					sdc = sdc_arr[i];
				}
			}
			return sdc;
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {			
			parse_arff();
			StringTokenizer itr = new StringTokenizer(value.toString());			
			while (itr.hasMoreTokens()) {				
				count.set(count.get()+1);
				context.write(count, new IntWritable(calc_dist(itr.nextToken())));
			}
		}
	}

	public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
			int sum = 0;

		}
	}

	public static void main(String[] args) throws Exception {		
		Configuration conf = new Configuration();
		//conf.set("tr", args[0]);
		FileSystem fs = FileSystem.getLocal(conf);
		Path output = new Path("output");
		fs.delete(output, true);
		
		Job job = Job.getInstance(conf, "MR-KNN");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		job.setJarByClass(MRkNN.class);
		job.setMapperClass(IdxMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);		

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
