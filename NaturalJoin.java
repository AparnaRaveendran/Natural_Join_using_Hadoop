/************************************************************************************************************************
*	Implementation of Natural Join of two relations using HADOOP MapReduce framework 				*
*	================================================================================				*
*	Input relations:												*
*	---------------													*
*		Orderdetails:  Order ID,Order Date, Ship Date, Ship Mode, Customer ID, Region, Product ID, Category, 	*
*				Sub-Category,Product Name,Sales, Quantity,Discount,Profit.				*
*		Customerdetails: Customer ID, Customer Name, Segment, Country,City,State,Postal Code			*
*															*
*	Key-attribute: Customer ID 											*
*	OrderMapper output: (Customer ID, ("order ",Order ID,Order Date, Ship Date, .....)				*
*	CustsMapper ouput:  (Customer ID, ("customer ",Customer Name, Segment, ...) 					*
*	NaturalJoinReducer: Iterates through the value-list of each key, and joins the corresponding tuples 		*
*				paired from each relation)								*
*	NaturalJoinReducer output: (Count, <joined tuple list>) for each key						*
*															*
*************************************************************************************************************************/




import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaturalJoin {
//Mapper class for Order_details
	public static class OrderMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			//System.out.println(record);
			String[] parts = record.split(",");
			//System.out.println("orderdetails:"+parts[4]);
			context.write(new Text(parts[4]), new Text("order  " + record));
		}
	}

//Mapper class for Customer details
	public static class CustsMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			//System.out.println(record);
			String[] parts = record.split(",",2);
			//System.out.println("custdetails:"+parts[0]);	
			context.write(new Text(parts[0]), new Text("customer   " + parts[1]));
		}
	}
//Reducer class
	public static class NaturalJoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			double total = 0.0;
			int count = 0;
			List<String> joinedOrderTuples = new ArrayList<>();
			List<String> joinedCustomerTuples = new ArrayList<>();
			List<String> joinedTuples = new ArrayList<>();
			//System.out.println("Key = "+key.toString());
			for (Text t : values) {
				String parts[] = t.toString().split("  ");
				//System.out.println("reducer details:"+","+parts[0]+","+ parts[1]);
				if (parts[0].equals("order")) 
					joinedOrderTuples.add(parts[1]);
				if (parts[0].equals("customer")) 
					joinedCustomerTuples.add(parts[1]);
			}
				for (int i = 0; i < joinedOrderTuples.size(); i++) 
					for (int j = 0; j < joinedCustomerTuples.size(); j++) {	
						joinedTuples.add(joinedOrderTuples.get(i)+","+joinedCustomerTuples.get(j));
						//System.out.println("joined tuples:"+joinedTuples.get(i));
						count++;
						
					}
			String key_count = String.format(key+" - %d\n", count);
			String str = "";
			for (int i = 0; i < joinedTuples.size(); i++) {
				str = str.concat(joinedTuples.get(i) +"\n");
			}
			//System.out.println( str);	
			context.write(new Text(key_count), new Text(str));
		}
	}

//main function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
   		Job job = Job.getInstance(conf, "NaturalJoin");
		job.setJarByClass(NaturalJoin.class);
		job.setReducerClass(NaturalJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OrderMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CustsMapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
