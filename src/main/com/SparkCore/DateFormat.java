package com.SparkCore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormat {

    public static void main(String[] args) throws ParseException {

        String s = "2019年9月6日,星期五,23:03:19";

        SimpleDateFormat sm = new SimpleDateFormat("YYYY年mm月dd日,E,hh:MM:ss");

        Date format = sm.parse(s);
        long time = format.getTime();
        System.out.println(time);
    }
}
