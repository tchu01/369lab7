// CSC 369 Winter 2016
// Multi Line JSON handling example
// Chris Wu

// run with  hadoop jar job.jar MultilineJsonJob -libjars /path/to/json-20151123.jar,/path/to/json-mapreduce-1.0.jar /input /output


import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.Iterator;

//import org.apache.hadoop.conf.Configuration;

//import org.apache.hadoop.conf.Configuration;

public class MultilineJsonJob extends Configured implements Tool {

   public static class JsonMapper
         extends Mapper<LongWritable, Text, Text, IntWritable> {

      private Text        outputKey   = new Text();
      private IntWritable outputValue = new IntWritable(1);

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         try {
            JSONObject json = new JSONObject(value.toString());
            Iterator<String> iter = json.keys();
            while (iter.hasNext()) {
               String jsonKey = iter.next();
               if ("yes".equals(json.getString(jsonKey))) {
                  outputKey.set(jsonKey);
                  context.write(outputKey, outputValue);
               } //if
            }   // while

         } catch (Exception e) {System.out.println(e); }
      }
   }

   public static class JsonReducer
         extends Reducer<Text, IntWritable, Text, IntWritable> {
      private IntWritable result = new IntWritable();

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         result.set(sum);
         context.write(key, result);
      }
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = super.getConf();
      Job job = Job.getInstance(conf, "multiline json job");

      job.setJarByClass(MultilineJsonJob.class);
      job.setMapperClass(JsonMapper.class);
      job.setReducerClass(JsonReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setInputFormatClass(MultiLineJsonInputFormat.class);
      MultiLineJsonInputFormat.setInputJsonMember(job, "game");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      int res = ToolRunner.run(conf, new MultilineJsonJob(), args);
      System.exit(res);
   }
}