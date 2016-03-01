// Author: Timothy Chu
// CPE369 - Section 01

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Scanner;

public class popular extends Configured implements Tool {

   public static class JsonMapper
         extends Mapper<LongWritable, Text, Text, Text> {

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         try {
            JSONObject json = new JSONObject(value.toString());
            if (json.has("text")) {
               String temp = json.getString("text");
               Scanner scan = new Scanner(temp);
               while (scan.hasNext()) {
                  String tempWord = scan.next();
                  context.write(new Text(tempWord), new Text(1 + ""));
               }
            }
         } catch (Exception e) {
            System.out.println(e);
         }
      }
   }

   public static class JsonReducer
         extends Reducer<Text, Text, Text, Text> {

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
         try {
            int sum = 0;
            for (Text val : values) {
               sum++;
            }
            context.write(key, new Text(sum + ""));
         } catch (Exception e) {
         }
      }
   }

   public class IntComparator extends WritableComparator {

      private String value;

      public IntComparator() {
         super(IntWritable.class);
      }

      @Override
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
         Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
         Integer v2 = ByteBuffer.wrap(b2,s2,l2).getInt();

         return v1.compareTo(v2) * (-1);
      }
   }


   public static class sortMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         Scanner scanner = new Scanner(value.toString());
         context.write(new Text(scanner.next()), new IntWritable(scanner.nextInt()));
      }
   }

   public static class sortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

      @Override
      protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         for (IntWritable num : values) {
            context.write(key, num);
         }
      }
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = super.getConf();
      Job job = Job.getInstance(conf, "multiline json job");

      job.setJarByClass(popular.class);
      job.setMapperClass(JsonMapper.class);
      job.setReducerClass(JsonReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setInputFormatClass(MultiLineJsonInputFormat.class);
      MultiLineJsonInputFormat.setInputJsonMember(job, "user");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception {
//      if (args.length != 2) {
//         System.out.println("Input format is: <input file name> <output directory name>\n");
//         System.exit(-1);
//      }

      //RUN JSON MAP-REDUCE JOB
      Configuration conf = new Configuration();
      int res = ToolRunner.run(conf, new popular(), args);

      Job sortJob = Job.getInstance();
      sortJob.setJarByClass(popular.class);
      FileInputFormat.addInputPath(sortJob, new Path("lab7/output9/part-r-00000")); // put what you need as input file
      FileOutputFormat.setOutputPath(sortJob, new Path("lab7/output9/output")); // put what you need as output file
      sortJob.setMapperClass(sortMapper.class);
      sortJob.setReducerClass(sortReducer.class);
      sortJob.setMapOutputKeyClass(Text.class);
      sortJob.setMapOutputValueClass(IntWritable.class);
      sortJob.setSortComparatorClass(IntComparator.class);
      sortJob.setOutputKeyClass(Text.class);
      sortJob.setOutputValueClass(IntWritable.class);
      sortJob.setOutputVIntComparator.class);
      sortJob.setJobName("Sort");

      System.exit(sortJob.waitForCompletion(true) ? 0 : 1);

      System.exit(res);

   }


}