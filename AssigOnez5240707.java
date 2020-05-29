import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;		//for sorting the string array(consisting of movie id) in the second mapper
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class AssigOnez5240707 {
	
	//Custom Writable for getting the movie-rating pair in the form (MovieId,rating) by overriding the toString() method
	public static class MRWritable implements Writable {
	    private Text first;
	    private int second;

	    public  MRWritable() {
	    	set(new Text(), second);
	    	
	    }
	    public  MRWritable(Text first, int second) {
	    	set(first,second);
	    }
	    
	    public Text getFirst() {
	        return first;
	    }
	    public void set(Text left, int right) {
	    	first = left;
	    	second = right;
	    }
	    
	    
	    public int getSecond() {
	        return second;
	    }
	    
	    
	    @Override
	    public void write(DataOutput out) throws IOException {
	        this.first.write(out);
	        out.writeInt(second);
	    }
	    @Override
	    public void readFields(DataInput in) throws IOException,NullPointerException {
	    	this.first.readFields(in);
	    	second = in.readInt();
	    }

	    @Override
	    public String toString() {
	        return  "(" + first + "," + second + ")";
	    }
	}
	
	//Custom ArrayWritable to integrate the pairs into a list format [(pair1),(pair2)..]
	public static class StringArrayWritable extends ArrayWritable{
		
		private ArrayList<String> array = new ArrayList<String>();
		int n;
		
		public StringArrayWritable() {
			super(Text.class);
			set(new Text[]{});
		}
		
		public StringArrayWritable(ArrayList<String> array) {
			this();
			int n = array.size();
			Text [] elements = new Text[n];
			for(int i=0;i<n;i++){
				elements[i] = new Text(array.get(i));
			}
			set(elements);
		}
		
		public StringArrayWritable(Text[] array) {
			this();
			set(array);
		}
		
		public String[] toStrings(){
			Writable [] array = get();
			int n = array.length;
			
			String[] elements = new String[n];
			for(int i=0;i<n;i++){
				elements[i] = ((Text)array[i]).toString();
			}
			return elements;
		}
		
		public String toString(){
			return "["+ StringUtils.join(toStrings(), ",") + "]";
		}
		
		@Override
		public void readFields(DataInput in) throws IOException{
			for(int i=0;i<n;i++) {
				new Text(this.array.get(i)).readFields(in);
			}
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			for(int i=0;i<n;i++) {
				new Text(this.array.get(i)).write(out);
			}
		}
	}
	
	//Custom Writable Comparable for forming the movie pairs (M1,M2),(M2,M3)... as the key
	public static class KeyWritable implements WritableComparable<KeyWritable>{
		private Text m1;
		private Text m2;
		
		public KeyWritable() {
			set(new Text(), new Text());
		}
		
		public KeyWritable(Text m1, Text m2) {
			set(new Text(m1), new Text(m2));
		}
		
		public Text getm1(){
			return m1;
		}
		
		public Text getm2() {
			return m2;
		}
		
		public void set(Text m1, Text m2) {
			this.m1 = m1;
			this.m2 = m2;
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			m1.write(out);
			m2.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException{
			m1.readFields(in);
			m2.readFields(in);
		}
		
		@Override
		public String toString() {
			return "(" + m1 + "," + m2 + ")";
		}
		
		@Override
		public int compareTo(KeyWritable mp) {
			int cmp = m1.compareTo(mp.m1);
			if(cmp!=0) {
				return cmp;
			}
			return m2.compareTo(mp.m2);
		}
	}

	/*Mapper-1: Takes the input text file and takes the input as LongWritable(index no) and Text(file contents)
	 as input and produces Text(userId) as key and movieId and rating pair as custom writable(MRWritable) value as output*/
	public static class UserMapper extends Mapper<LongWritable, Text, Text, MRWritable>{
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, MRWritable>.Context context)
		throws IOException, InterruptedException{
			String[] tokens = value.toString().split("::");
			MRWritable movieRate = new MRWritable();
			//sending the movie id(tokens[1]) and rating(tokens[2]) into the custom writable to form a pair
			movieRate.set(new Text(tokens[1]), Integer.parseInt(tokens[2]));
			//producing the mapper output with the userid and the pair
			context.write(new Text(tokens[0]),movieRate);			
		}
	}

	/*Reducer-1: Takes the mapper output as input which consists of the userId and movie-rating pair
	 and converts them(movie-rating pair) into an array by sending the value into the StringArrayWritable
	 and producing Text(userId) as output key and StringArrayWritable(movie-rating pairs array) as output value */
	public static class UserReducer extends Reducer<Text,MRWritable,Text,StringArrayWritable>{
		@Override
		protected void reduce(Text key, Iterable<MRWritable> values, 
				Reducer<Text,MRWritable,Text,StringArrayWritable>.Context context) 
				throws IOException, InterruptedException{
			//Array List to add the movie-rating pair to further send it into the StringArrayWritable
			ArrayList<String> temp_list = new ArrayList<String>();
			for(MRWritable mr_pair:values) {
				temp_list.add(mr_pair.toString());
			}
			StringArrayWritable array = new StringArrayWritable(temp_list);
			//Writing the key(userId) and the value(array) as the output of the reducer
			context.write(key, array);
			
		}
	}

	/*Mapper-2: Takes the input file containing the userId and the movie-rating pair and
	 *  Uses the split and reduce functions to clean the input file to extact the movieId and ratings
	 *  Iterate over the formed list to add the movieId's into another list and form a hash map
	 *  The hash map contains the movieId's associated with it's respective rating
	 *  Then sort the movie_array and using 2 for loops to form the movie pairs as key using the custom WritableComparable
	 *  The using the hash map constructed to form a final key-value pair as movie pairs-(userId,rating1,rating2)*/
	public static class MovieMapper extends Mapper<LongWritable,Text,KeyWritable,Text>{
		KeyWritable mpairs = new KeyWritable(); //custom WritableComparable
		private static HashMap<String,Integer> hm = new HashMap<String,Integer>(); //Hash map declaration
		
		@Override
		protected void map(LongWritable key,Text values,
				Mapper<LongWritable,Text,KeyWritable,Text>.Context context)
				throws IOException,InterruptedException{
			ArrayList<String> movie_array = new ArrayList<String>(); //array to store the movieid's
			
			String[] parts = values.toString().split("\t");
			
			String pairs = parts[1].replace("[","").replace("]", "").replace("),"," ").replace("(", "").replace(")", "");
			String[] temp = pairs.split(" ");
			for(String s:temp) {
				String[] ele = s.split(",");
				movie_array.add(ele[0]); //adding the movieId into the array
				hm.put(ele[0], Integer.parseInt(ele[1])); //forming the hash map with movieId and it's corresponding rating
			} 
			Collections.sort(movie_array); //sorting the array
			
			for(int i=0; i<movie_array.size()-1; i++) {
				String movie1 = movie_array.get(i);
				for(int j=i+1; j<movie_array.size();j++) {
					String movie2 = movie_array.get(j);
					mpairs.set(new Text(movie1), new Text(movie2));
					String the_value = String.format("%s,%d,%d", key.toString(),hm.get(movie1),hm.get(movie2)); //forming the value of the mapper(userId,rating1,rating2)
					String final_string = "(" + the_value +")";
					//writing the output key(movie pairs)-value(userId,rating1,rating2) as the mapper output
					context.write(mpairs, new Text(final_string));
				}
			}
		}
	}

	/*Reducer-2: Takes the output from the mapper((movie pairs) (userId,rating1,rating2)..) and
	 * uses the custom ArrayWritable to add that value into the array
	 * Produces final output in the format:
 				(movie1,movie2)	[(userId1,rating1,rating2),(userId2,rating1,rating2)] */
	public static class MovieReducer extends Reducer<KeyWritable,Text,KeyWritable,StringArrayWritable>{
		
		@Override
		protected void reduce(KeyWritable key, Iterable<Text> values,
				Reducer<KeyWritable,Text,KeyWritable,StringArrayWritable>.Context context)
				throws IOException, InterruptedException{
			//List to add the values from the mapper to eventually send it into the custom ArrayWritable
			ArrayList<String> final_array = new ArrayList<String>();
			
			for(Text pair:values) {
				final_array.add(pair.toString());
			}
			StringArrayWritable array = new StringArrayWritable(final_array);
			//Produces the final output in the key-value(in array format)
			context.write(key, array);
		}
	}
		

	/*Main function: Does chaining of two jobs with 2 map-reduce operations and 
	 * writes the output of the first job into "out2"(file)
	 * writes the output of the second job into "out3" (file)*/
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		Job job1 = Job.getInstance(conf, "Assign");
		job1.setJarByClass(AssigOnez5240707.class);
		
		job1.setMapperClass(UserMapper.class);
		job1.setReducerClass(UserReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MRWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(StringArrayWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(out, "out2"));
		if(!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		Job job2 = Job.getInstance(conf,"users and movie pairs");
		job2.setJarByClass(AssigOnez5240707.class);
		
		job2.setMapperClass(MovieMapper.class);
		job2.setReducerClass(MovieReducer.class);
		
		job2.setMapOutputKeyClass(KeyWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(KeyWritable.class);
		job2.setOutputValueClass(StringArrayWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(out, "out2"));
		FileOutputFormat.setOutputPath(job2, new Path(out, "out3"));
		if(!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
	}

}
