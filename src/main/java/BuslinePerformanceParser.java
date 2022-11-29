import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class BuslinePerformanceParser {
    private String lineName;
    private String dayOfWeek;
    private int totalGetOnNum = 0;
    private int totalGetOffNum = 0;


    // 역별 승하차 인원 총합
    // 첫 번째 줄 제외
    public BuslinePerformanceParser(Text text) {
        try {
            String[] columns = text.toString().split(",");
            lineName = columns[2];
            totalGetOnNum += Integer.parseInt(columns[6]);
            totalGetOnNum += Integer.parseInt(columns[7]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String strDate = columns[0];
            Date date = new Date(sdf.parse(strDate).getTime());

            SimpleDateFormat newFormat = new SimpleDateFormat("E", Locale.KOREA);
            dayOfWeek = newFormat.format(date);

        } catch (Exception e) {
            System.out.println("Error parsing a record" + e.getMessage());
        }
    }

    public int getTotalGetOnNum() {
        return totalGetOnNum;
    }

    public int getTotalGetOffNum() {
        return totalGetOffNum;
    }

    public String getLineName() {
        return lineName;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }
}
