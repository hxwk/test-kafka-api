import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateUtil {
    /**
     * All Dates are normalized to UTC, it is up the client code to convert to the appropriate TimeZone.
     */
    public static final TimeZone UTC;


    /**
     * @see <a href="http://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations">Combined Date and Time Representations</a>
     */
    public static final String ISO_8601_24H_FULL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";


    /**
     * 0001-01-01T00:00:00.000Z
     */
    public static final Date BEGINNING_OF_TIME;


    /**
     * 292278994-08-17T07:12:55.807Z
     */
    public static final Date END_OF_TIME;

    static {
        UTC = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(UTC);
        final Calendar c = new GregorianCalendar(UTC);
        c.set(1, 0, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        BEGINNING_OF_TIME = c.getTime();
        c.setTime(new Date(Long.MAX_VALUE));
        END_OF_TIME = c.getTime();
    }

    /**
     * 返回修正的时间字符串
     *
     * @param now     为传入的时间字符串
     * @param seconds 修正的秒
     * @return 根据修正秒返回的时间字符串
     */
    public static String getRightTime(String now, Integer seconds) {
        final SimpleDateFormat sdf = new SimpleDateFormat(ISO_8601_24H_FULL_FORMAT);
        sdf.setTimeZone(UTC);
        Date date = new Date();
        try {
            sdf.parse(now);
        } catch (ParseException e) {
            System.out.println(e.getErrorOffset());
        }
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(date);
        rightNow.add(Calendar.SECOND, seconds);
        System.out.println("sdf.format(new Date()) = " + sdf.format(date));
        System.out.println("sdf.format(rightNow) = " + sdf.format(rightNow.getTime()));
        return sdf.format(rightNow.getTime());
    }

    public static void main(String[] args) {
        final SimpleDateFormat sdf = new SimpleDateFormat(ISO_8601_24H_FULL_FORMAT);
        sdf.setTimeZone(UTC);
        String rightTime = getRightTime("2019-11-26T00:50:45.516Z", 3);
        System.out.println("rightTime: " + rightTime);
    }
}
