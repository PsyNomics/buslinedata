package example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserCountDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(), new UserCountDriver(), args);
        System.out.println("MapReduce Job Result: " + exitCode);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: example.hadoop.BusLineUserCountDriver <input> <output>");
            System.exit(2);
        }

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "BusLineUserCount");
        job.setJarByClass(UserCountDriver.class);
        job.setMapperClass(UserCountMapper.class);
        job.setReducerClass(UserCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }
}