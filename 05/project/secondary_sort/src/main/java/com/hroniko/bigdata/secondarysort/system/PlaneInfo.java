package com.hroniko.bigdata.secondarysort.system;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Класс информации о самолете и его рейсе
public class PlaneInfo implements Writable {

    private LongWritable year = new LongWritable(); // Год
    private Text model = new Text(); // Название модели
    private Text issueDate = new Text(); // Дата выпуска
    private Text manufacturer = new Text(); // Производстводитель
    private LongWritable flightNum = new LongWritable(); // Количество рейсов
    private LongWritable cancelledFlightNum = new LongWritable(); // Отменен ли рейс
    private DoubleWritable hourseInAir = new DoubleWritable(); // Количество часов в воздухе
    private DoubleWritable distance = new DoubleWritable(); // Пройденное расстояние в км


    public PlaneInfo(LongWritable year, Text model, Text issueDate, Text manufacturer, LongWritable flightNum, LongWritable cancelledFlightNum, DoubleWritable hourseInAir, DoubleWritable distance) {
        this.year = year;
        this.model = model;
        this.issueDate = issueDate;
        this.manufacturer = manufacturer;
        this.flightNum = flightNum;
        this.cancelledFlightNum = cancelledFlightNum;
        this.hourseInAir = hourseInAir;
        this.distance = distance;
    }

    public PlaneInfo() {
        this.year = new LongWritable(0); // Год
        this.model = new Text("NA"); // Название модели
        this.issueDate = new Text("NA"); // Дата выпуска
        this.manufacturer = new Text("NA"); // Производстводитель
        this.flightNum = new LongWritable(0); // Количество рейсов
        this.cancelledFlightNum = new LongWritable(0); // Отменен ли рейс
        this.hourseInAir = new DoubleWritable(0.0); // Количество часов в воздухе
        this.distance = new DoubleWritable(0.0); // Пройденное расстояние в км
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        year.write(dataOutput);
        model.write(dataOutput);
        issueDate.write(dataOutput);
        manufacturer.write(dataOutput);
        flightNum.write(dataOutput);
        cancelledFlightNum.write(dataOutput);
        hourseInAir.write(dataOutput);
        distance.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year.readFields(dataInput);
        model.readFields(dataInput);
        issueDate.readFields(dataInput);
        manufacturer.readFields(dataInput);
        flightNum.readFields(dataInput);
        cancelledFlightNum.readFields(dataInput);
        hourseInAir.readFields(dataInput);
        distance.readFields(dataInput);
    }

    @Override
    public String toString() { // Будем использовать в редьюссере уже
        return  model.toString() + "," +
                issueDate.toString() + "," +
                manufacturer.toString();
    }

    public String toInfo() { // Используем в маппере при формировании составного ключа
        return  model.toString() + "," +
                issueDate.toString() + "," +
                manufacturer.toString();
    }


    public LongWritable getYear() {
        return year;
    }

    public void setYear(LongWritable year) {
        this.year = year;
    }

    public Text getModel() {
        return model;
    }

    public void setModel(Text model) {
        this.model = model;
    }

    public Text getIssueDate() {
        return issueDate;
    }

    public void setIssueDate(Text issueDate) {
        this.issueDate = issueDate;
    }

    public Text getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(Text manufacturer) {
        this.manufacturer = manufacturer;
    }

    public LongWritable getFlightNum() {
        return flightNum;
    }

    public void setFlightNum(LongWritable flightNum) {
        this.flightNum = flightNum;
    }

    public LongWritable getCancelledFlightNum() {
        return cancelledFlightNum;
    }

    public void setCancelledFlightNum(LongWritable cancelledFlightNum) {
        this.cancelledFlightNum = cancelledFlightNum;
    }

    public DoubleWritable getHourseInAir() {
        return hourseInAir;
    }

    public void setHourseInAir(DoubleWritable hourseInAir) {
        this.hourseInAir = hourseInAir;
    }

    public DoubleWritable getDistance() {
        return distance;
    }

    public void setDistance(DoubleWritable distance) {
        this.distance = distance;
    }
}
