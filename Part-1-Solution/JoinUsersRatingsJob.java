import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinUsersRatingsJob {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "JoinUsersRatingsJob");
    job.setJarByClass(JoinUsersRatings.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    MultipleInputs.addInputPath(
        job,
        new Path(args[0]),
        TextInputFormat.class,
        UsersMapper.class
    );
    MultipleInputs.addInputPath(
        job,
        new Path(args[1]),
        TextInputFormat.class,
        RatingsMapper.class
    );

    job.setReducerClass(JoinUsersRatingsReducer.class);

    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class UsersMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final USER_TAG = new Text("U");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueSplitted = value.toString().split("\t");

      int age = Integer.parseInt(valueSplitted[2]);

      if (age > 25) {
        Text keyToEmit = new Text(valueSplitted[0]);

        context.write(keyToEmit, USER_TAG);
      }
    }
  }

  public static class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueSplitted = value.toString().split("\t");

      int rating = Integer.parseInt(valueSplitted[2]);

      if (rating > 2) {
        Text keyToEmit = new Text(valueSplitted[0]);

        String valueToEmitFormatted = String.format("R\t%s\t%s", valueSplitted[1], valueSplitted[2]);

        Text valueToEmit = new Text(valueToEmitFormatted);

        context.write(keyToEmit, valueToEmit);
      }
    }
  }

  public static class JoinUsersRatingsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      boolean userIsFound = false;

      List<String[]> tuplesToEmit = new ArrayList<>();

      for (Text value : values) {
        if (value.toString().equals("U")) {
          userIsFound = true;
        } else {
          String[] valueSplitted = value.toString().split("\t");
          
          String[] movieIDRatingTuple = {
              valueSplitted[1],
              valueSplitted[2]
          };

          tuplesToEmit.add(movieIDRatingTuple);
        }
      }

      if (!userIsFound) {
        return;
      }

      for (String[] tuple : tuplesToEmit) {
        Text keyToEmit = new Text(tuple[0]);
        
        Text valueToEmit = new Text(tuple[1]);
        
        context.write(keyToEmit, valueToEmit);
      }
    }
  }
}
