// Author: Timothy Chu
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
import java.util.*;

public class hashtags extends Configured implements Tool {

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
            Set<String> stopwords = new HashSet<>();
            stopwords.add("a");
            stopwords.add("the");
            stopwords.add("in");
            stopwords.add("on");
            stopwords.add("I");
            stopwords.add("he");
            stopwords.add("she");
            stopwords.add("it");
            stopwords.add("there");
            stopwords.add("is");
            HashMap<String, Integer> words = new HashMap<>();
            int maxCount = -1;

            for(Text val: values) {
               Scanner scan = new Scanner(val.toString());
               while(scan.hasNext()) {
                  String str = scan.next();
                  if(words.containsKey(str)) {
                     words.put(str, words.get(str) + 1);
                  } else {
                     words.put(str, 1);

                  }

                  if(maxCount < words.get(str)) {
                     maxCount = words.get(str);
                  }
               }
            }
            String returnString = "";
            Iterator<String> keys = words.keySet().iterator();
            while(keys.hasNext()) {
               String temp = keys.next();
               if(words.get(temp) == maxCount && !stopwords.contains(temp)) {
                  returnString += temp + ", ";
               }
            }
            returnString = returnString.substring(0, returnString.length()-2);
            context.write(key, new Text(returnString));
         } catch (Exception e) {
         }
      }
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = super.getConf();
      Job job = Job.getInstance(conf, "multiline json job");

      job.setJarByClass(hashtags.class);
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
      int res = ToolRunner.run(conf, new hashtags(), args);
      System.exit(res);

   }


}