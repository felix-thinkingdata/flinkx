package com.dtstack.flinkx.connector.ta.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    public enum DateType {DATE, DATETIME_SECOND, DATETIME_MS, NOT_DATE}

    private static final DateFormat dayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat secondDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat msecDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateFormat hourDateFormat = new SimpleDateFormat("yyyyMMddHH");

    private static DateFormat partitionDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat shortDateFormat = new SimpleDateFormat("yyyyMMdd");
    private static DateFormat longDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    private static DateFormat preciseDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static DateType getDateType(String str) {
        if (str == null) {
            return DateType.NOT_DATE;
        } else if (str.length() == 10) {
            for (int i = 0; i < str.length(); i++) {
                if (!isVaildDateChar(str.charAt(i), i)) {
                    return DateType.NOT_DATE;
                }
            }
            return DateType.DATE;
        } else if (str.length() == 19) {
            for (int i = 0; i < str.length(); i++) {
                if (!isVaildDateChar(str.charAt(i), i)) {
                    return DateType.NOT_DATE;
                }
            }
            return DateType.DATETIME_SECOND;
        } else if (str.length() == 23) {
            for (int i = 0; i < str.length(); i++) {
                if (!isVaildDateChar(str.charAt(i), i)) {
                    return DateType.NOT_DATE;
                }
            }
            return DateType.DATETIME_MS;
        }
        return DateType.NOT_DATE;
    }

    public synchronized static Date parserDateStr(String str) {
        try {
            if (str.length() == 10) {
                return dayDateFormat.parse(str);
            } else if (str.length() == 19) {
                return secondDateFormat.parse(str);
            } else if (str.length() == 23) {
                return msecDateFormat.parse(str);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public synchronized static Date parseDateString(String dateStr) {
        int length = dateStr.length();
        DateFormat format;

        if (8 == length) {
            format = shortDateFormat;
            try {
                return format.parse(dateStr);
            } catch (ParseException e) {

            }
        } else if (14 == length) {
            format = longDateFormat;
            try {
                return format.parse(dateStr);
            } catch (ParseException e) {

            }
        } else if (10 == length) {
            format = partitionDateFormat;
            try {
                return format.parse(dateStr);
            } catch (ParseException e) {

            }

            format = new SimpleDateFormat("yyyy/MM/dd");
            try {
                return format.parse(dateStr);
            } catch (ParseException e) {

            }
        } else if (19 == length) {
            format = preciseDateFormat;
            try {
                return format.parse(dateStr);
            } catch (ParseException e) {

            }
        } else if (21 == length) {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            try {
                return format.parse(dateStr);
            } catch (ParseException e) {

            }
        } else if (23 == length) {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            try {
                return format.parse(dateStr);
            } catch (ParseException e) {

            }
        }

        throw new RuntimeException("can not parse date string :" + dateStr);
    }

    public synchronized static String formatHourPatternDate(Date date) {
        return hourDateFormat.format(date);
    }

    public synchronized static String getLastPointHourDate(Date date, int intervalHour) {
        long timeMs = date.getTime();
        long hourPointMs = timeMs - (timeMs + 28800000) % (intervalHour * 3600000);
        return hourDateFormat.format(new Date(hourPointMs));
    }

    public synchronized static Date getLastPointMinutesDate(Date date, int intervalMinutes) {
        long timeMs = date.getTime();
        long minutePointMs = timeMs - (timeMs + 28800000) % (intervalMinutes * 60000);
        return new Date(minutePointMs);
    }
    public synchronized static String getPreciseDateString(Date date){
        return preciseDateFormat.format(date);
    }
    public synchronized static String getLastPointHourDateAhead(Date date, int intervalHour) {
        long timeMs = date.getTime();
        long hourPointMs = timeMs - (timeMs + 28800000) % (intervalHour * 3600000) - (intervalHour * 3600000);
        return hourDateFormat.format(new Date(hourPointMs));
    }

    public synchronized static String formatDateByFormatStr(Date date, String formatStr) {
        if ("yyyy-MM-dd HH:mm:ss.SSS".equals(formatStr)) {
            return formatMsPatterndate(date);
        } else {
            return formatSecondPatterndate(date);

        }

    }


    public synchronized static String formatSecondPatterndate(Date date) {
        return secondDateFormat.format(date);
    }

    public synchronized static String formatDatePatterndate(Date date) {
        return dayDateFormat.format(date);
    }

    public synchronized static String formatMsPatterndate(Date date) {
        return msecDateFormat.format(date);
    }


    public synchronized static String getOffsetDatePartitionString(Date currentDate, int offsetDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);
        calendar.add(Calendar.DATE, offsetDate);
        return dayDateFormat.format(calendar.getTime());
    }

    public synchronized static Date getOffsetDate(Date currentDate, int offsetDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);
        calendar.add(Calendar.DATE, offsetDate);
        return calendar.getTime();
    }

    public synchronized static Date getOffsetHourDate(Date date, int offsetHour) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR, offsetHour);
        return calendar.getTime();
    }

    public synchronized static Date getOffsetMinuteDate(Date date, int offsetMinute) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, offsetMinute);
        return calendar.getTime();
    }

    private static boolean isVaildDateChar(char c, int index) {
        if (index == 0) {
            return (c >= '1' && c <= '2');
        } else if (index >= 1 && index <= 3) {
            return (c >= '0' && c <= '9');
        } else if (index == 4 || index == 7) {
            return c == '-';
        } else if (index == 5) {
            return (c >= '0' && c <= '1');
        } else if (index == 6 || index == 9 || index == 12 || index == 15 || index == 18) {
            return (c >= '0' && c <= '9');
        } else if (index == 8) {
            return (c >= '0' && c <= '3');
        } else if (index == 10) {
            return c == ' ';
        } else if (index == 11) {
            return (c >= '0' && c <= '2');
        } else if (index == 13 || index == 16) {
            return c == ':';
        } else if (index == 14 || index == 17) {
            return (c >= '0' && c <= '5');
        } else if (index == 19) {
            return c == '.';
        } else if (index >= 20 && index <= 22) {
            return (c >= '0' && c <= '9');
        } else {
            return false;
        }

    }
}
