package com.MapReduce;

import java.text.DecimalFormat;

public class Decimalformat {


    public static void main(String[] args) {

        double f = 132.361;

        DecimalFormat format = new DecimalFormat("#.0");
        String resul = format.format(f);

        double end = Double.parseDouble(resul);

        System.out.println(end);//132.4

    }
}
