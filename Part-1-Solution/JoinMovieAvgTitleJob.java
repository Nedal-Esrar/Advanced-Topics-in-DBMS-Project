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

public class JoinMovieAvgTitleJob {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "JoinMovieAvgTitleJob");
    job.setJarByClass(JoinMovieAvgTitle.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    MultipleInputs.addInputPath(
        job,
        new Path(args[0]),
        TextInputFormat.class,
        MovieMapper.class
    );
    MultipleInputs.addInputPath(
        job,
        new Path(args[1]),
        TextInputFormat.class,
        MovieRatingMapper.class
    );

    job.setReducerClass(JoinAvgTitleReducer.class);

    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueSplitted = value.toString().split("\t");

      String genres = valueSplitted[2];

      if (genres.equalsIgnoreCase("children") || genres.equalsIgnoreCase("comedy")) {
        Text keyToEmit = new Text(valueSplitted[0]);

        Text valueToEmit = new Text("M\t" + valueSplitted[1]);

        context.write(keyToEmit, valueToEmit);
      }
    }
  }

  public static class MovieRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueSplitted = value.toString().split("\t");

      Text keyToEmit = new Text(valueSplitted[0]);

      Text valueToEmit = new Text("A\t" + valueSplitted[1]);

      context.write(keyToEmit, valueToEmit);
    }
  }

  public static class JoinAvgTitleReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String title = null;

      long sm = 0, cnt = 0;

      for (Text value : values) {
        String[] valueSplitted = value.toString().split("\t");

        if (valueSplitted[0].equals("M")) {
          title = valueSplitted[1];
        } else {
          ++cnt;

          sm += Integer.parseInt(valueSplitted[1]);
        }
      }

      if (title == null || cnt == 0) {
        return;
      }

      Text valueToEmit = new Text(title + "\t" + 1.0f * sm / cnt);

      context.write(key, valueToEmit);
    }
  }
}