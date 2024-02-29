package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SelfJoinWeights extends Configured implements Tool {

    public static class WeightsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length == 3) {
                context.write(new Text(parts[0]), new Text(parts[1] + "," + parts[2]));
            }
        }
    }

    public static class WeightsReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> weights = new ArrayList<>();
            for (Text value : values) {
                weights.add(value.toString());
            }

            for (String weight1 : weights) {
                for (String weight2 : weights) {
                    context.write(new Text(key.toString() + "," + weight1 + "," + weight2), NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SelfJoinWeights <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Self Join Weights");
        job.setJarByClass(SelfJoinWeights.class);
        job.setMapperClass(WeightsMapper.class);
        job.setReducerClass(WeightsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SelfJoinWeights(), args);
        System.exit(exitCode);
    }
}
