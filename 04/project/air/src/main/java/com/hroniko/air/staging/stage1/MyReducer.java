package com.hroniko.air.staging.stage1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer <Text, Plane, Text, Text>{
    final static String SEP = ",";

    public void setup (Context context) {

    }


    @Override
    protected void reduce(Text key, Iterable<Plane> values, Context context) throws IOException, InterruptedException {

        Text dim_model = new Text("");
        Text dim_issue_date = new Text("");
        Text dim_manufacturer = new Text("");

        Text flightNum  = new Text("");
        Text cancelledFlightNum = new Text("");

        Integer flightsCount = 0; // количество рейсов за год
        Integer canceledFlightsCount = 0; // Количество отмененных рейсов
        Double airTime = 0.0; // время полета (поле AirTime) в часах
        Double distance = 0.0; // расстояние (из миль перевести в км)



        for (Plane plane : values) { // Для каждой записи о данной модели самолета

            dim_model = plane.getDim_model();
            dim_issue_date = plane.getDim_issue_date();
            dim_manufacturer = plane.getDim_manufacturer();

            flightNum = plane.getFlightNum();
            cancelledFlightNum = plane.getCancelled();

            if (cancelledFlightNum.toString().equals("0")){ // Если рейс не отменен, то
                airTime += Integer.parseInt(plane.getAirTime().toString());
                distance += Integer.parseInt(plane.getDistance().toString());
            }
            else{
                canceledFlightsCount ++; // Иначе, если отменен, то добавляем к количеству отмененных рейсов
            }
        }

        airTime /= 60; // От минут к часам
        distance *= 1.60934; // переводим мили в км


        String valueOut =
                    dim_model + SEP +
                    dim_issue_date + SEP +
                    dim_manufacturer + SEP +
                    flightNum + SEP +
                    canceledFlightsCount  + SEP +
                    airTime.toString() + SEP +
                    distance.toString();

        context.write(new Text(), new Text(valueOut));

    }
}
