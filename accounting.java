// Author: Timothy Chu & Michael Wong
// Lab 7
// CPE369 - Section 01

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import java.io.IOException;

public class accounting extends Configured implements Tool {

   public static class JsonMapper
         extends Mapper<LongWritable, Text, Text, Text> {

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         try {
            JSONObject json = new JSONObject(value.toString());
            context.write(new Text(json.getString("user")), new Text(json.getString("text")));
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
            double cost = 0;
            int count = 0;

            for (Text val : values) {
               cost += 0.05;
               String message = val.toString();
               cost += (Math.ceil(message.length() / 10.0) / 100.0);
               if (message.length() > 100) {
                  cost += 0.05;
               }
               count++;
            }
            if (count > 100) {
               cost = cost * 0.95;
            }
            context.write(key, new Text(cost + ""));
         } catch (Exception e) {
         }
      }
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = super.getConf();
      Job job = Job.getInstance(conf, "multiline json job");

      job.setJarByClass(accounting.class);
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
      //RUN JSON MAP-REDUCE JOB
      Configuration conf = new Configuration();
      int res = ToolRunner.run(conf, new accounting(), args);
      System.exit(res);
   }
}