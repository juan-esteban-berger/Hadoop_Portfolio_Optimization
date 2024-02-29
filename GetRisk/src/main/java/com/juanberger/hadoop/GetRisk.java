package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;

public class GetRisk implements Tool {

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static class RiskMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 7) {
                int portfolioID = Integer.parseInt(parts[0]);
                double weightedCov = Double.parseDouble(parts[6]);
                context.write(new IntWritable(portfolioID), new DoubleWritable(weightedCov));
            }
        }
    }

    public static class RiskReducer extends Reducer<IntWritable, DoubleWritable, Text, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sumWeightedCov = 0.0;
            for (DoubleWritable value : values) {
                sumWeightedCov += value.get();
            }
            double risk = Math.sqrt(sumWeightedCov * 252);
            DecimalFormat df = new DecimalFormat("#.######");
            context.write(new Text(key.toString() + "," + df.format(risk)), NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GetRisk <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(conf, "Calculate Risk of Portfolios");
        job.setJarByClass(GetRisk.class);

        job.setMapperClass(RiskMapper.class);
        job.setReducerClass(RiskReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new GetRisk(), args);
        System.exit(exitCode);
    }
}
