package com.euyuil.forefinger.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Has a Mapper class and maps Maps (offset, line) to (identity, line).
 * Doesn't have Reducer class. Thus an identity key for each line would be in the output.
 * Identity is a value that can identify the table.
 *
 * Input view could be a ViewMetaData.
 *
 * TODO Maybe projection and condition could be applied.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.29
 */
public class IdentityViewMapReduce extends ViewMapReduce {

    private static final String PARAM_IDENTITY = "identity";

    /**
     * Maps (offset, line) to (identity, line).
     */
    public static class IdentityViewMapper extends ViewMapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable longWritable = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            long identity = context.getConfiguration().getLong(PARAM_IDENTITY, -1);
            if (identity == -1)
                throw new IOException("Identity should be set and >= 0");
            longWritable.set(identity);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(longWritable, value);
        }
    }
}
