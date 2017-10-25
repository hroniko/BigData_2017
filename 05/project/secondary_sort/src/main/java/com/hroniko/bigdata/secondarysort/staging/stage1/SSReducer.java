package com.hroniko.bigdata.secondarysort.staging.stage1;

import com.hroniko.bigdata.secondarysort.system.ComparedKey;
import com.hroniko.bigdata.secondarysort.system.PlaneInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SSReducer extends Reducer<ComparedKey, PlaneInfo, NullWritable, Text> {


    @Override
    protected void reduce(ComparedKey key, Iterable<PlaneInfo> values, Context context) throws IOException, InterruptedException {

        if (key.getState().get() != 2000L) return; // Если в составном ключе нет 2000 года, просто выходим

        // Иначе считаем
        long flightNumPeriod = 0L; // общее количество рейсов
        long cancelledFlightNumPeriod = 0L; // общее количество отмененных рейсов
        double hoursInAirPeriod = 0.0; // общее время полетов
        double distancePeriod = 0.0; // общее расстояние перелетов


        for (PlaneInfo planeInfo : values) { // Для каждой записи о полете самолета
            long flightNum = planeInfo.getFlightNum().get();
            long cancelledFlightNum = planeInfo.getCancelledFlightNum().get();
            double hoursInAir = planeInfo.getHourseInAir().get();
            double distance = planeInfo.getDistance().get();

            flightNumPeriod += flightNum; // Увеличиваем количество рейсов
            cancelledFlightNumPeriod += cancelledFlightNum;

            if (cancelledFlightNum == 0L){ // Если рейс не отменен, то
                hoursInAirPeriod += hoursInAir;
                distancePeriod += distance;
            }
        }

        // if (hoursInAirPeriod == 0.0) return;

        String valueOut =
                key.toString() + "," +
                        flightNumPeriod + "," +
                        cancelledFlightNumPeriod + "," +
                        hoursInAirPeriod + "," +
                        distancePeriod;


        context.write(NullWritable.get(), new Text(valueOut));

    }
}
