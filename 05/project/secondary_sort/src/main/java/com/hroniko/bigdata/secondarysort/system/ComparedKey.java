package com.hroniko.bigdata.secondarysort.system;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Класс составного ключа для Secondary Sort в mapReduce
public class ComparedKey  implements WritableComparable {

    private Text key = new Text(); // Естественный ключ, у нас это инфа о самолете
    // (year, dim.model, dim.issue_date, dim.manufacturer), но можно и целиком

    private LongWritable state = new LongWritable(); // compared state // тут будет год, по которому будем сортировать


    public ComparedKey(){
    }

    public ComparedKey(Text key, LongWritable state) {
        this.key = key;
        this.state = state;
    }

    public void write(DataOutput out) throws IOException {
        key.write(out);
        state.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        state.readFields(in);
    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public LongWritable getState() {
        return state;
    }

    public void setState(LongWritable comparedState) {
        this.state = comparedState;
    }

    @Override
    public int compareTo(Object o) {
        ComparedKey key2 = (ComparedKey) o;

        int cmp = key.compareTo(key2.getKey()); // Сравниваем описания самолетов
        if (cmp != 0) { // если не равны, то смысла дальше нет сравнивать
            return cmp;
        }
        // А если же равны, то больший из них тот, у кого год больше
        return getState().compareTo(key2.getState()); // т.е. иначе, если совпали описания самолета, сравниваем года
    }

    @Override
    public String toString() {
        return key.toString();
    }
}
