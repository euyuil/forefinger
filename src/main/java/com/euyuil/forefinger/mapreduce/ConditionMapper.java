package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.condition.Condition;
import com.euyuil.forefinger.DataRow;
import com.euyuil.forefinger.serde.RowSerDe;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.09
 */
public class ConditionMapper extends
        Mapper<LongWritable, Text, LongWritable, Text> {

    private RowSerDe rowSerDe;
    private Text serialized = new Text();
    private Condition condition;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        DataRow dataRow = rowSerDe.deserialize(line);
        if (condition.fulfilled(dataRow)) {
            serialized.set(rowSerDe.serialize(dataRow));
            context.write(key, serialized);
        }
    }
}
