package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.DataRow;
import com.euyuil.forefinger.serde.RowSerDe;
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

    private RowSerDe rowSerDe;
    private Text serialized = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        DataRow dataRow = rowSerDe.deserialize(line);
        serialized.set(rowSerDe.serialize(dataRow));
        context.write(key, serialized);
    }
}
