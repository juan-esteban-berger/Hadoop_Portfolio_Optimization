package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ExpectedReturn extends Configured implements Tool {

    public static class ExpectedReturnMapper extends Mapper<Object, Text, Text, FloatWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length == 5) {
		// PortfolioID, Stock, Weight, Mean_Return, Weighted_Return
                context.write(new Text(parts[0]), new FloatWritable(Float.parseFloat(parts[4])));
            }
        }
    }

    public static class ExpectedReturnReducer extends Reducer<Text, FloatWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable value : values) {
                sum += value.get();
            }
            float annualSum = sum * 252;
	    // Weighted_Return, Expected_Return
            String result = String.format("%.6f,%.6f", sum, annualSum);
            context.write(new Text(key.toString() + ',' + result), NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ExpectedReturn <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ExpectedReturn");
        job.setJarByClass(ExpectedReturn.class);
        job.setMapperClass(ExpectedReturnMapper.class);
        job.setReducerClass(ExpectedReturnReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ExpectedReturn(), args);
        System.exit(exitCode);
    }
}
