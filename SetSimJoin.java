package comp9313.ass4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * comp9313 Ass4
 * Date: 24/05/2017  Author: Hui Jia ID:z5077824
 * 
 * This assignment is mainly about compute the set similarity by using map reduce.
 * Note that the whole process should contains:
 * 		Stage_1: Sort the all the elementId by their frequency
 * 		Stage_2: Load the order of stage1, and reorder the element respect stage_1 order
 * 		Stage_3: Remove duplicate and sort as pair and output
 * 
 * However, lecturer has said all the elementId have been sorted. Therefore I start from stage_2 directly.
 * 
 * 		Stage_2:
 * 			Mapper:
 * 				1. Compute prefix token length(length filter)
 * 						Len = r - ceil(r * sim) + 1
 * 				2. emit(prefix_token, all_record)
 * 
 * 			Reducer:
 * 				1. Get the recordId
 * 				2. compute Jaccard Similarity
 * 				3. write to HDFS file
 * 
 *		Stage_3:
 *				1. first create pair class(int id_1, int id_2), and overwrite the hashcode and compareTo function
 *				Mapper:
 *					1.Read from HDFS and emit(pair(id_1, id_2), similiarity)
 *
 *				Reducer:
 *					1.Form the output format and write to output folder
 * 
 */


public class SetSimJoin {
	
	
/*	Stage_2:
 * 			Mapper:
 * 				1. Compute prefix token length(length filter)
 * 						Len = r - ceil(r * sim) + 1
 * 				2. emit(prefix_token, all_record)
 * 
 */
	public static class PartitionMapper extends Mapper<Object, Text, Text, Text> {	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			ArrayList<String> list = new ArrayList<String>();
		
			Double threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
			
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
			while (itr.hasMoreTokens()) {
				list.add(itr.nextToken());
			}
				
			int i, j = 0;
			String elements = list.toString();
			elements = elements.substring(1, elements.length() - 1);

			double num = list.size() - 1 - Math.ceil((list.size() - 1) * threshold) + 1;
			
			for(i = 1; i <= num; i++){
				context.write(new Text(list.get(i)), new Text(elements));

			} 
		}		
	}

/*Stage_2:
 * 		Reducer:
 * 				1. Get the recordId
 * 				2. compute Jaccard Similarity
 * 				3. write to HDFS file
 */
	
	public static class PartitionReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Double threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
			
			ArrayList<String> list = new ArrayList<String>();
			HashSet<String> table = new HashSet<String>();
			
			int id1 = 0;
			int id2 = 0;
			double intersect = 0.0;
			double similarity = 0.0;
			
			for (Text val : values) {
				list.add(val.toString());
			}
			
			for(int i = 0; i < list.size() - 1; i++){
				for(int j = i+1; j < list.size(); j++){
					
					table.clear();
					intersect = 0; 
					
					String arr1[] = list.get(i).split(", ");
					String arr2[] = list.get(j).split(", ");
					
					id1 = Integer.parseInt(arr1[0]);
					id2 = Integer.parseInt(arr2[0]);
					
					// compute Jaccard Similarity
					for(int k = 1; k < arr1.length; k++){
						table.add(arr1[k]);
					}
					for(int k = 1; k < arr2.length; k++){
						if(table.contains(arr2[k])){
							intersect++;
						}else{
							table.add(arr2[k]);
						}
					}
					similarity = intersect / table.size();
					
					//3. write to HDFS file
					if(similarity >= threshold){
						if(id1 < id2){
							context.write(new Text(id1 + ""), new Text(id2 + "\t" + similarity));
						}else{
							context.write(new Text(id2 + ""), new Text(id1 + "\t" + similarity));
						}	
						
					}
				}
			}
		}
	}

	/* Stage_3
	 * 		Mapper:
	 *					1.Read from HDFS and emit(pair(id_1, id_2), similiarity)
	 */
	public static class PairMapper extends Mapper<Object, Text, IdPair, Text> {	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			ArrayList<String> list = new ArrayList<String>();
			StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			
			while (itr.hasMoreTokens()) {
				list.add(itr.nextToken());
			}
		
			context.write(new IdPair(Integer.parseInt(list.get(0)), Integer.parseInt(list.get(1))), new Text(list.get(2)));
		}		
	}
	
	/* Stage_3
	 *		Reducer:
	 *			1.Form the output format and write to output folder
	 */
	public static class PairReducer extends Reducer<IdPair, Text, Text, Text> {

		public void reduce(IdPair pair, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String sim = "";
			for (Text val : values) {
				sim = val.toString();
				break;
			}
			
			context.write(new Text("(" + pair.getId_1()+ "," + pair.getId_2() + ")"), new Text(sim));
		}
	}
	
	/*
	 * create pair class(int id_1, int id_2), and overwrite the hashcode and compareTo function
	 */
	// construct the pair class to store the length and count for each key
	public static class IdPair implements WritableComparable<IdPair>{

		private int id_1, id_2;
		public IdPair(){
			
		}
		public IdPair(int id_1, int id_2){
			set(id_1, id_2);
		}
		
		public void set(int id_1, int id_2){
			this.id_1 = id_1;
			this.id_2 = id_2;
		}
		
		public int getId_1(){
			return id_1;
		}
		
		public int getId_2(){
			return id_2;
		}
		
		public void write(DataOutput out) throws IOException{
	
			out.writeInt(id_1);
			out.writeInt(id_2);
		}
		
		public void readFields(DataInput in) throws IOException{
			id_1 = in.readInt();
			id_2 = in.readInt();
		}
		@Override
		// first compare id_1, and compare id_2
		public int compareTo(IdPair pair) {
			if(id_1 == pair.getId_1()){
				return (id_2 - pair.getId_2());
			}
			return (id_1 - pair.getId_1());
		}
		
		// return id_1 as hashcode, convenient for partitioner
		public int hashCode(){
			return id_1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		// set similarity and number of reducers
		String threshold = args[2];
		int tasks = Integer.parseInt(args[3]);
		
		conf.set("threshold", threshold);
		
		Job job = Job.getInstance(conf, "word count");
		job.setNumReduceTasks(tasks);
	
		// stage_2 map reduce
		String input = args[0];
		String output = args[1] + System.nanoTime();
		 
		job.setJarByClass(SetSimJoin.class);
		job.setMapperClass(PartitionMapper.class);
		job.setReducerClass(PartitionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		// stage_3 map reduce
		input = output;
		output = args[1];
	
		job = Job.getInstance(conf, "word count");
		job.setNumReduceTasks(tasks);
		job.setJarByClass(SetSimJoin.class);
		job.setMapperClass(PairMapper.class);
		job.setReducerClass(PairReducer.class);
		job.setOutputKeyClass(IdPair.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
