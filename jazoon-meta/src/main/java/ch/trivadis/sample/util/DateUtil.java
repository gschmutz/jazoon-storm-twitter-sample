package ch.trivadis.sample.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;


public class DateUtil {
	private final static Logger logger = Logger.getLogger(DateUtil.class);

    public static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    private static ThreadLocal<DateFormat> df = new ThreadLocal<DateFormat> () {

    	  @Override
    	  public DateFormat get() {
    	   return super.get();
    	  }

    	  @Override
    	  protected DateFormat initialValue() {
    	   return new SimpleDateFormat(DateUtil.DATE_PATTERN);
    	  }

    	  @Override
    	  public void remove() {
    	   super.remove();
    	  }

    	  @Override
    	  public void set(DateFormat value) {
    	   super.set(value);
    	  }

    };

    public static String getDateAsString(Date date) {
        return df.get().format(date);
    }

    public static Date toDate(String startDate)  {
    	if (startDate != null && startDate.length() > 0) {
    		try{
    			return df.get().parse(startDate);
    		}
    		catch(NumberFormatException nfe) {
    			return null;
    		}
        	catch(ParseException e) {
        		throw new RuntimeException(e);
        	}
    		catch(ArrayIndexOutOfBoundsException e) {
    			logger.error("Invalid startDate in DateUtil.toDate(): " + startDate);
    			return null;
        	}
    	} else {
    		return null;
    	}
    	
    }

    public static Date currentDateOnMidnight() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

}
