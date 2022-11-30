package example.multiple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public String workType = null;
    private Text outputKey = new Text();
    private  IntWritable outputValue = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LineParser parser = new LineParser(value);

        outputKey.set("on," + parser.getDayOfWeek() + "," + parser.getLineName());
        outputValue.set(parser.getTotalGetOnNum());
        context.write(outputKey, outputValue);

        outputKey.set("off," + parser.getDayOfWeek() + "," + parser.getLineName());
        outputValue.set(parser.getTotalGetOffNum());
        context.write(outputKey, outputValue);

        outputKey.set("total," + parser.getDayOfWeek() + "," + parser.getLineName());
        outputValue.set(parser.getTotalGetOnNum() + parser.getTotalGetOffNum());
        context.write(outputKey, outputValue);
    }

}
