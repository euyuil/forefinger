package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.serde.DataRow;
import com.euyuil.forefinger.serde.DataSerDe;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This mapper just echo every line.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.09
 */
public class EchoMapper extends
        Mapper<LongWritable, Text, LongWritable, Text> {

    private DataSerDe dataSerDe;
    private Text serialized = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        DataRow dataRow = dataSerDe.deserialize(line);
        serialized.set(dataSerDe.serialize(dataRow));
        context.write(key, serialized);
    }
}
