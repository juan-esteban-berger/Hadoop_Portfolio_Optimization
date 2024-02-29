package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinCovWeights extends Configuration implements Tool {

    private Configuration conf = new Configuration();

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Join Covariances with Weights");
        job.setJarByClass(JoinCovWeights.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JoinCovWeights(), args);
        System.exit(exitCode);
    }

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length == 3) {
                context.write(new Text(parts[0] + "," + parts[1]), new Text("cov," + parts[2]));
            } else if (parts.length == 5) {
                context.write(new Text(parts[1] + "," + parts[3]), new Text("weight," + parts[0] + "," + parts[2] + "," + parts[4]));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String cov = null;
            List<String> weightList = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if ("cov".equals(parts[0])) {
                    cov = parts[1];
                } else if ("weight".equals(parts[0])) {
                    weightList.add(parts[1] + "," + key.toString() + "," + parts[2] + "," + parts[3]);
                }
            }

            for (String weightValues : weightList) {
                if (cov != null) {
                    context.write(new Text(weightValues + "," + cov), NullWritable.get());
                } else {
                    context.write(new Text(weightValues + ",null"), NullWritable.get());
                }
            }
        }
    }
}
