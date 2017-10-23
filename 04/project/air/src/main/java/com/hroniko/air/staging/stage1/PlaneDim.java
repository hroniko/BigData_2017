package com.hroniko.air.staging.stage1;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Класс одна запись словарика Plane-data
public class PlaneDim implements Writable {

    private Text tailnum; // 0
    private Text type;
    private Text manufacturer;
    private Text issue_date;
    private Text model;
    private Text status;
    private Text aircraft_type;
    private Text engine_type;
    private Text year;  // 8

    public PlaneDim(Text stringOfParams, Text separator){

        // разделяем входную строку по сепараторы
        String string = stringOfParams.toString();
        String sep = separator.toString();
        String[] sts = string.split(sep); // Массив слов

        for(int i = 0; i < sts.length; i++){
            if (sts[i].equals("NA")) sts[i] = "0";
        }

        // System.out.println(sts.length);

        this.tailnum = new Text("0");
        this.type = new Text("0");
        this.manufacturer = new Text("0");
        this.issue_date = new Text("0");
        this.model = new Text("0");
        this.status = new Text("0");
        this.aircraft_type = new Text("0");
        this.engine_type = new Text("0");
        this.year = new Text("0");


        if (sts.length > 0) this.tailnum = new Text(sts[0]);
        if (sts.length > 1) this.type = new Text(sts[1]);
        if (sts.length > 2) this.manufacturer = new Text(sts[2]);
        if (sts.length > 3) this.issue_date = new Text(sts[3]);
        if (sts.length > 4) this.model = new Text(sts[4]);
        if (sts.length > 5) this.status = new Text(sts[5]);
        if (sts.length > 6) this.aircraft_type = new Text(sts[6]);
        if (sts.length > 7) this.engine_type = new Text(sts[7]);
        if (sts.length > 8) this.year = new Text(sts[8]);


    }

    public PlaneDim(){
        this.tailnum = new Text("0");
        this.type = new Text("0");
        this.manufacturer = new Text("0");
        this.issue_date = new Text("0");
        this.model = new Text("0");
        this.status = new Text("0");
        this.aircraft_type = new Text("0");
        this.engine_type = new Text("0");
        this.year = new Text("0");
    }


    public Text getTailnum() {
        return tailnum;
    }

    public Text getType() {
        return type;
    }

    public Text getManufacturer() {
        return manufacturer;
    }

    public Text getIssue_date() {
        return issue_date;
    }

    public Text getModel() {
        return model;
    }

    public Text getStatus() {
        return status;
    }

    public Text getAircraft_type() {
        return aircraft_type;
    }

    public Text getEngine_type() {
        return engine_type;
    }

    public Text getYear() {
        return year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        tailnum.write(dataOutput);
        type.write(dataOutput);
        manufacturer.write(dataOutput);
        issue_date.write(dataOutput);
        model.write(dataOutput);
        status.write(dataOutput);
        aircraft_type.write(dataOutput);
        engine_type.write(dataOutput);
        year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tailnum.readFields(dataInput);
        type.readFields(dataInput);
        manufacturer.readFields(dataInput);
        issue_date.readFields(dataInput);
        model.readFields(dataInput);
        status.readFields(dataInput);
        aircraft_type.readFields(dataInput);
        engine_type.readFields(dataInput);
        year.readFields(dataInput);
    }

    @Override
    public String toString() {
        return  tailnum + "," +
                type + "," +
                manufacturer + "," +
                issue_date + "," +
                model + "," +
                status + "," +
                aircraft_type + "," +
                engine_type + "," +
                year;
    }
}
