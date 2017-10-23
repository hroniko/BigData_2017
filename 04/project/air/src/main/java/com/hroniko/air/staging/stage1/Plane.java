package com.hroniko.air.staging.stage1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


// Класс Самолета
public class Plane implements Writable {

    // Отключаем неиспользуемые поля и методы ///

    ///private Text Year; //0
    ///private Text Month;
    ///private Text DayofMonth;
    ///private Text DayOfWeek;
    ///private Text DepTime;
    ///private Text CRSDepTime;
    ///private Text ArrTime;
    ///private Text CRSArrTime;
    ///private Text UniqueCarrier;
    private Text FlightNum; // 9
    private Text TailNum; // 10
    ///private Text ActualElapsedTime;
    ///private Text CRSElapsedTime;
    private Text AirTime; // 13
    ///private Text ArrDelay;
    ///private Text DepDelay;
    ///private Text Origin;
    ///private Text Dest;
    private Text Distance; // 18
    ///private Text TaxiIn;
    ///private Text TaxiOut;
    private Text Cancelled; //21
    ///private Text CancellationCode;
    ///private Text Diverted;
    ///private Text CarrierDelay;
    ///private Text WeatherDelay;
    ///private Text NASDelay;
    ///private Text SecurityDelay;
    ///private Text LateAircraftDelay;  // 28

    // А тут поля, относящиеся к словарю:

    ///private Text dim_tailnum; // 0
    ///private Text dim_type;
    private Text dim_manufacturer; // 2
    private Text dim_issue_date; // 3
    private Text dim_model; // 4
    ///private Text dim_status;
    ///private Text dim_aircraft_type;
    ///private Text dim_engine_type;
    ///private Text dim_year;  // 8



    public Plane(){
        ///this.Year = new Text("0");
        ///this.Month = new Text("0");
        ///this.DayofMonth = new Text("0");
        ///this.DayOfWeek = new Text("0");
        ///this.DepTime = new Text("0");
        ///this.CRSDepTime = new Text("0");
        ///this.ArrTime = new Text("0");
        ///this.CRSArrTime = new Text("0");
        ///this.UniqueCarrier = new Text("0");
        this.FlightNum = new Text("0");
        this.TailNum = new Text("0");
        ///this.ActualElapsedTime = new Text("0");
        ///this.CRSElapsedTime = new Text("0");
        this.AirTime = new Text("0");
        ///this.ArrDelay = new Text("0");
        ///this.DepDelay = new Text("0");
        ///this.Origin = new Text("0");
        ///this.Dest = new Text("0");
        this.Distance = new Text("0");
        ///this.TaxiIn = new Text("0");
        ///this.TaxiOut = new Text("0");
        this.Cancelled = new Text("0");
        ///this.CancellationCode = new Text("0");
        ///this.Diverted = new Text("0");
        ///this.CarrierDelay = new Text("0");
        ///this.WeatherDelay = new Text("0");
        ///this.NASDelay = new Text("0");
        ///this.SecurityDelay = new Text("0");
        ///this.LateAircraftDelay = new Text("0");



        ///this.dim_tailnum = new Text("0");
        ///this.dim_type = new Text("0");
        this.dim_manufacturer = new Text("0");
        this.dim_issue_date = new Text("0");
        this.dim_model = new Text("0");
        ///this.dim_status = new Text("0");
        ///this.dim_aircraft_type = new Text("0");
        ///this.dim_engine_type = new Text("0");
        ///this.dim_year = new Text("0");

    }


    public Plane(Text stringOfParams, Text separator) {

        ///this.Year = new Text("0");
        ///this.Month = new Text("0");
        ///this.DayofMonth = new Text("0");
        ///this.DayOfWeek = new Text("0");
        ///this.DepTime = new Text("0");
        ///this.CRSDepTime = new Text("0");
        ///this.ArrTime = new Text("0");
        ///this.CRSArrTime = new Text("0");
        ///this.UniqueCarrier = new Text("0");
        this.FlightNum = new Text("0");
        this.TailNum = new Text("0");
        ///this.ActualElapsedTime = new Text("0");
        ///this.CRSElapsedTime = new Text("0");
        this.AirTime = new Text("0");
        ///this.ArrDelay = new Text("0");
        ///this.DepDelay = new Text("0");
        ///this.Origin = new Text("0");
        ///this.Dest = new Text("0");
        this.Distance = new Text("0");
        ///this.TaxiIn = new Text("0");
        ///this.TaxiOut = new Text("0");
        this.Cancelled = new Text("0");
        ///this.CancellationCode = new Text("0");
        ///this.Diverted = new Text("0");
        ///this.CarrierDelay = new Text("0");
        ///this.WeatherDelay = new Text("0");
        ///this.NASDelay = new Text("0");
        ///this.SecurityDelay = new Text("0");
        ///this.LateAircraftDelay = new Text("0");


        ///this.dim_tailnum = new Text("0");
        ///this.dim_type = new Text("0");
        this.dim_manufacturer = new Text("0");
        this.dim_issue_date = new Text("0");
        this.dim_model = new Text("0");
        ///this.dim_status = new Text("0");
        ///this.dim_aircraft_type = new Text("0");
        ///this.dim_engine_type = new Text("0");
        ///this.dim_year = new Text("0");


        // разделяем входную строку по сепараторы, но сначала в String
        String string = stringOfParams.toString();
        String sep = separator.toString();
        String[] sts = string.split(sep); // Массив слов

        for(int i = 0; i < sts.length; i++){
            if (sts[i].equals("NA")) sts[i] = "0";
        }

        ///if (sts.length > 0) this.Year = new Text(sts[0]);
        ///if (sts.length > 1) this.Month = new Text(sts[1]);
        ///if (sts.length > 2) this.DayofMonth = new Text(sts[2]);
        ///if (sts.length > 3) this.DayOfWeek = new Text(sts[3]);
        ///if (sts.length > 4) this.DepTime = new Text(sts[4]);
        ///if (sts.length > 5) this.CRSDepTime = new Text(sts[5]);
        ///if (sts.length > 6) this.ArrTime = new Text(sts[6]);
        ///if (sts.length > 7) this.CRSArrTime = new Text(sts[7]);
        ///if (sts.length > 8) this.UniqueCarrier = new Text(sts[8]);
        if (sts.length > 9) this.FlightNum = new Text(sts[9]);
        if (sts.length > 10) this.TailNum = new Text(sts[10]);
        ///if (sts.length > 11) this.ActualElapsedTime = new Text(sts[11]);
        ///if (sts.length > 12) this.CRSElapsedTime = new Text(sts[12]);
        if (sts.length > 13) this.AirTime = new Text(sts[13]);
        ///if (sts.length > 14) this.ArrDelay = new Text(sts[14]);
        ///if (sts.length > 15) this.DepDelay = new Text(sts[15]);
        ///if (sts.length > 16) this.Origin = new Text(sts[16]);
        ///if (sts.length > 17) this.Dest = new Text(sts[17]);
        if (sts.length > 18) this.Distance = new Text(sts[18]);
        ///if (sts.length > 19) this.TaxiIn = new Text(sts[19]);
        ///if (sts.length > 20) this.TaxiOut = new Text(sts[20]);
        if (sts.length > 21) this.Cancelled = new Text(sts[21]);
        ///if (sts.length > 22) this.CancellationCode = new Text(sts[22]);
        ///if (sts.length > 23) this.Diverted = new Text(sts[23]);
        ///if (sts.length > 24) this.CarrierDelay = new Text(sts[24]);
        ///if (sts.length > 25) this.WeatherDelay = new Text(sts[25]);
        ///if (sts.length > 26) this.NASDelay = new Text(sts[26]);
        ///if (sts.length > 27) this.SecurityDelay = new Text(sts[27]);
        ///if (sts.length > 28) this.LateAircraftDelay = new Text(sts[28]);


    }

    /*
    public Text getYear() {
        return Year;
    }

    public Text getMonth() {
        return Month;
    }

    public Text getDayofMonth() {
        return DayofMonth;
    }

    public Text getDayOfWeek() {
        return DayOfWeek;
    }

    public Text getDepTime() {
        return DepTime;
    }

    public Text getCRSDepTime() {
        return CRSDepTime;
    }

    public Text getArrTime() {
        return ArrTime;
    }

    public Text getCRSArrTime() {
        return CRSArrTime;
    }

    public Text getUniqueCarrier() {
        return UniqueCarrier;
    }
    */

    public Text getFlightNum() {
        return FlightNum;
    }

    public Text getTailNum() {
        return TailNum;
    }


    /*
    public Text getActualElapsedTime() {
        return ActualElapsedTime;
    }

    public Text getCRSElapsedTime() {
        return CRSElapsedTime;
    }

    */

    public Text getAirTime() {
        return AirTime;
    }

    /*

    public Text getArrDelay() {
        return ArrDelay;
    }

    public Text getDepDelay() {
        return DepDelay;
    }

    public Text getOrigin() {
        return Origin;
    }

    public Text getDest() {
        return Dest;
    }

    */

    public Text getDistance() {
        return Distance;
    }

    /*

    public Text getTaxiIn() {
        return TaxiIn;
    }

    public Text getTaxiOut() {
        return TaxiOut;
    }

    */

    public Text getCancelled() {
        return Cancelled;
    }

    /*

    public Text getCancellationCode() {
        return CancellationCode;
    }

    public Text getDiverted() {
        return Diverted;
    }

    public Text getCarrierDelay() {
        return CarrierDelay;
    }

    public Text getWeatherDelay() {
        return WeatherDelay;
    }

    public Text getNASDelay() {
        return NASDelay;
    }

    public Text getSecurityDelay() {
        return SecurityDelay;
    }

    public Text getLateAircraftDelay() {
        return LateAircraftDelay;
    }

    */

    ///


    /*

    public Text getDim_tailnum() {
        return dim_tailnum;
    }

    public Text getDim_type() {
        return dim_type;
    }

    */

    public Text getDim_manufacturer() {
        return dim_manufacturer;
    }

    public Text getDim_issue_date() {
        return dim_issue_date;
    }

    public Text getDim_model() {
        return dim_model;
    }

    /*

    public Text getDim_status() {
        return dim_status;
    }

    public Text getDim_aircraft_type() {
        return dim_aircraft_type;
    }

    public Text getDim_engine_type() {
        return dim_engine_type;
    }

    public Text getDim_year() {
        return dim_year;
    }

    */




    /// Вставка словаря


    public void setPlaneDim(PlaneDim planeDim) {

        ///this.dim_tailnum = planeDim.getTailnum();
        ///this.dim_type = planeDim.getType();
        this.dim_manufacturer = planeDim.getManufacturer();
        this.dim_issue_date = planeDim.getIssue_date();
        this.dim_model = planeDim.getModel();
        ///this.dim_status = planeDim.getStatus();
        ///this.dim_aircraft_type = planeDim.getAircraft_type();
        ///this.dim_engine_type = planeDim.getEngine_type();
        ///this.dim_year = planeDim.getYear();

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ///Year.write(dataOutput);
        ///Month.write(dataOutput);
        ///DayofMonth.write(dataOutput);
        ///DayOfWeek.write(dataOutput);
        ///DepTime.write(dataOutput);
        ///CRSDepTime.write(dataOutput);
        ///ArrTime.write(dataOutput);
        ///CRSArrTime.write(dataOutput);
        ///UniqueCarrier.write(dataOutput);
        FlightNum.write(dataOutput);
        TailNum.write(dataOutput);
        ///ActualElapsedTime.write(dataOutput);
        ///CRSElapsedTime.write(dataOutput);
        AirTime.write(dataOutput);
        ///ArrDelay.write(dataOutput);
        ///DepDelay.write(dataOutput);
        ///Origin.write(dataOutput);
        ///Dest.write(dataOutput);
        Distance.write(dataOutput);
        ///TaxiIn.write(dataOutput);
        ///TaxiOut.write(dataOutput);
        Cancelled.write(dataOutput);
        ///CancellationCode.write(dataOutput);
        ///Diverted.write(dataOutput);
        ///CarrierDelay.write(dataOutput);
        ///WeatherDelay.write(dataOutput);
        ///NASDelay.write(dataOutput);
        ///SecurityDelay.write(dataOutput);
        ///LateAircraftDelay.write(dataOutput);

        ///dim_tailnum.write(dataOutput);
        ///dim_type.write(dataOutput);
        dim_manufacturer.write(dataOutput);
        dim_issue_date.write(dataOutput);
        dim_model.write(dataOutput);
        ///dim_status.write(dataOutput);
        ///dim_aircraft_type.write(dataOutput);
        ///dim_engine_type.write(dataOutput);
        ///dim_year.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ///Year.readFields(dataInput);
        ///Month.readFields(dataInput);
        ///DayofMonth.readFields(dataInput);
        ///DayOfWeek.readFields(dataInput);
        ///DepTime.readFields(dataInput);
        ///CRSDepTime.readFields(dataInput);
        ///ArrTime.readFields(dataInput);
        ///CRSArrTime.readFields(dataInput);
        ///UniqueCarrier.readFields(dataInput);
        FlightNum.readFields(dataInput);
        TailNum.readFields(dataInput);
        ///ActualElapsedTime.readFields(dataInput);
        ///CRSElapsedTime.readFields(dataInput);
        AirTime.readFields(dataInput);
        ///ArrDelay.readFields(dataInput);
        ///DepDelay.readFields(dataInput);
        ///Origin.readFields(dataInput);
        ///Dest.readFields(dataInput);
        Distance.readFields(dataInput);
        ///TaxiIn.readFields(dataInput);
        ///TaxiOut.readFields(dataInput);
        Cancelled.readFields(dataInput);
        ///CancellationCode.readFields(dataInput);
        ///Diverted.readFields(dataInput);
        ///CarrierDelay.readFields(dataInput);
        ///WeatherDelay.readFields(dataInput);
        ///NASDelay.readFields(dataInput);
        ///SecurityDelay.readFields(dataInput);
        ///LateAircraftDelay.readFields(dataInput);


        ///dim_tailnum.readFields(dataInput);
        ///dim_type.readFields(dataInput);
        dim_manufacturer.readFields(dataInput);
        dim_issue_date.readFields(dataInput);
        dim_model.readFields(dataInput);
        ///dim_status.readFields(dataInput);
        ///dim_aircraft_type.readFields(dataInput);
        ///dim_engine_type.readFields(dataInput);
        ///dim_year.readFields(dataInput);

    }


    @Override
    public String toString() {
        return ///Year + ", " +
        ///Month + ", " +
        ///DayofMonth + ", " +
        ///DayOfWeek + ", " +
        ///DepTime + ", " +
        ///CRSDepTime + ", " +
        ///ArrTime + ", " +
        ///CRSArrTime + ", " +
        ///UniqueCarrier + ", " +
        FlightNum + ", " +
        TailNum + ", " +
        ///ActualElapsedTime + ", " +
        ///CRSElapsedTime + ", " +
        AirTime + ", " +
        ///ArrDelay + ", " +
        ///DepDelay + ", " +
        ///Origin + ", " +
        ///Dest + ", " +
        Distance + ", " +
        ///TaxiIn + ", " +
        ///TaxiOut + ", " +
        Cancelled; /// + ", " +
        ///CancellationCode + ", " +
        ///Diverted + ", " +
        ///CarrierDelay + ", " +
        ///WeatherDelay + ", " +
        ///NASDelay + ", " +
        ///SecurityDelay + ", " +
        ///LateAircraftDelay;
    }
}
