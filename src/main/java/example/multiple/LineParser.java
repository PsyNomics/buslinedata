package example.multiple;

import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class LineParser {
    private String lineName;
    private String dayOfWeek;
    private int totalGetOnNum = 0;
    private int totalGetOffNum = 0;

    public LineParser(Text text) {
        try {
            String[] columns = text.toString().split(",");
            lineName = columns[2];
            totalGetOnNum = Integer.parseInt(columns[6]);
            totalGetOffNum = Integer.parseInt(columns[7]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String strDate = columns[0];
            Date date = new Date(sdf.parse(strDate).getTime());

            SimpleDateFormat newFormat = new SimpleDateFormat("E", Locale.KOREA);
            dayOfWeek = newFormat.format(date);

        } catch (Exception e) {
            System.out.println("Error parsing a record" + e.getMessage());
        }
    }

    public String getLineName() {
        return lineName;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public int getTotalGetOnNum() {
        return totalGetOnNum;
    }

    public int getTotalGetOffNum() {
        return totalGetOffNum;
    }

}
