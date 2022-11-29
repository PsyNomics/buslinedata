import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class Test {
    public static void main(String[] args) throws Throwable {
        String[] columns = {"20221001","5530","5530번"};
        String dayOfWeek;

//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.KOREA);
//        LocalDate date = LocalDate.parse(columns[0], formatter);
//        System.out.println(date);



        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String strDate = columns[0];
        Date date = new Date(sdf.parse(strDate).getTime());
        System.out.println(date);
//        String[] week = {"일요일", "월요일", "화요일", "수요일", "목요일", "금요일", "토요일"};
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(date);
        SimpleDateFormat newFormat = new SimpleDateFormat("E", Locale.KOREA);
        System.out.println(newFormat.format(date));
        dayOfWeek = newFormat.format(date);
        System.out.println(dayOfWeek);
    }


}
