package example.multiple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class UserCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private MultipleOutputs<Text, IntWritable> mos;
    private Text outputKey = new Text();
    private IntWritable result = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] columns = key.toString().split(",");

        outputKey.set(columns[1] + "," + columns[2]);

        int sum = 0;
        for (IntWritable value : values)
            sum += value.get();
        result.set(sum);

        if (columns[0].equals("on")) {
            mos.write("GetOnNum", outputKey, result);
        } else if (columns[0].equals("off")) {
            mos.write("GetOffNum", outputKey, result);
        } else if (columns[0].equals("total")) {
            mos.write("totalNum", outputKey, result);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
