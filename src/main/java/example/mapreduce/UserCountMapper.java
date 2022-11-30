package example.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public String workType = null;
    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        workType = context.getConfiguration().get("workType");
        if (workType == null) { // -D workType 옵션을 넣지 않았을 때 Error: java.lang.NullPointerException 방지
            workType = "";
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LineParser parser = new LineParser(value);

        if (workType.equals("getOn")) {
            outputKey.set(parser.getDayOfWeek() + "," + parser.getLineName());
            outputValue.set(parser.getTotalGetOnNum());
            context.write(outputKey, outputValue);
        } else if (workType.equals("getOff")) {
            outputKey.set(parser.getDayOfWeek() + "." + parser.getLineName());
            outputValue.set(parser.getTotalGetOffNum());
            context.write(outputKey, outputValue);
        } else {
            // -D workType 옵션에 다른 값 혹은 철자 틀렸을 경우 자동으로 total 합계가 실행되도록 정의
            // else if로 변경후 -D workType에 다른 값을 넣을 경우 아무 출력X
            outputKey.set(parser.getDayOfWeek() + "," + parser.getLineName());
            outputValue.set(parser.getTotalGetOnNum() + parser.getTotalGetOffNum());
            context.write(outputKey, outputValue);
        }


    }}
