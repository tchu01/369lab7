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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class activity extends Configured implements Tool {

   public static class JsonMapper
         extends Mapper<LongWritable, Text, Text, Text> {

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         try {
            JSONObject json = new JSONObject(value.toString());
            context.write(new Text(json.getString("user") + ""), value);
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

         HashMap<Integer, List<JSONObject>> games = new HashMap<>();
         int won = 0;
         int lost = 0;
         int highScore = 0;
         int longestGame = 0;

         try {
            for (Text val : values) {
               JSONObject json = new JSONObject(val.toString());
               List<JSONObject> list;
               if (!games.containsKey(json.getInt("game"))) {
                  list = new ArrayList<>();
               } else {
                  list = games.get(json.getInt("game"));
               }
               list.add(json);
               games.put(json.getInt("game"), list);

               if (json.getJSONObject("action").has("gameStatus")) {
                  if (json.getJSONObject("action").getInt("actionNumber") > longestGame) {
                     longestGame = json.getJSONObject("action").getInt("actionNumber");
                  }

                  if (json.getJSONObject("action").getString("gameStatus").equals("Loss")) {
                     lost++;
                  } else {
                     won++;
                     if (json.getJSONObject("action").getInt("points") > highScore) {
                        highScore = json.getJSONObject("action").getInt("points");
                     }
                  }
               }
            }
         } catch (Exception e) {
            System.out.println(e.toString());
         }

         int gamesPlayed = games.size();
         JSONObject obj = new JSONObject();

         try {
            obj.put("games", gamesPlayed);
            obj.put("won", won);
            obj.put("lost", lost);
            obj.put("highscore", highScore);
            obj.put("longestGame", longestGame);
         } catch (Exception e) {
            System.out.println(e.toString());
         }
         context.write(key, new Text(obj.toString()));

      }
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = super.getConf();
      Job job = Job.getInstance(conf, "multiline json job");

      job.setJarByClass(activity.class);
      job.setMapperClass(JsonMapper.class);
      job.setReducerClass(JsonReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setInputFormatClass(MultiLineJsonInputFormat.class);
      MultiLineJsonInputFormat.setInputJsonMember(job, "action");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception {
      //RUN JSON MAP-REDUCE JOB
      Configuration conf = new Configuration();
      int res = ToolRunner.run(conf, new activity(), args);
      System.exit(res);
   }
}