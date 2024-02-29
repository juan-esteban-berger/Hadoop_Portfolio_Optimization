package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class GetCovDiv extends Configured implements Tool {

    public static class CovDivMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private double divisor;

        @Override
        public void setup(Context context) throws IOException {
            // Get the divisor from the configuration
            divisor = context.getConfiguration().getDouble("divisor", 1.0);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length == 3) {
                double covariance = Double.parseDouble(parts[2]);
                context.write(new Text(parts[0] + "," + parts[1]), new DoubleWritable(covariance / divisor));
            }
        }
    }

    public static class CovDivReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                context.write(new Text(key.toString() + "," + val.get()), NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: GetCovDiv <input path> <divisor> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        double divisor = Double.parseDouble(args[1]);
        conf.setDouble("divisor", divisor);

        Job job = Job.getInstance(conf, "Get Covariance Divided by Divisor");
        job.setJarByClass(GetCovDiv.class);
        job.setMapperClass(CovDivMapper.class);
        job.setReducerClass(CovDivReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GetCovDiv(), args);
        System.exit(exitCode);
    }
}
