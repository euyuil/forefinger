package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.condition.Condition;
import com.euyuil.forefinger.meta.view.ViewMetaData;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.serde.Serializer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.19
 */
public abstract class ViewMapReduce {

    public static final String PARAM_VIEW_NAME = "viewName";
    public static final String PARAM_SERIALIZER = "serializer";
    public static final String PARAM_DESERIALIZER = "deserializer";

    public static abstract class ViewMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
            extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        protected Text text = new Text();

        protected ViewMetaData viewMetaData;
        protected Serializer viewMetaDataSerializer;
        protected Deserializer viewMetaDataDeserializer;
        protected Condition viewMetaDataCondition;
        protected List<MetaDataColumn> viewMetaDataColumns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            String viewName = context.getConfiguration().get(PARAM_VIEW_NAME);
            if (viewName == null || viewName.length() == 0)
                throw new IllegalArgumentException("View name not specified");

            // System.out.println(String.format("View name is %s", viewName));

            viewMetaData = MetaDataSet.getDefault().getMetaData(viewName, ViewMetaData.class);

            viewMetaDataSerializer = viewMetaData.getSerializer();
            viewMetaDataDeserializer = viewMetaData.getDeserializer();
            viewMetaDataCondition = viewMetaData.getCondition();
            viewMetaDataColumns = viewMetaData.getMetaDataColumns();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static abstract class ViewReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
            extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        protected Text text = new Text();

        protected ViewMetaData viewMetaData;
        protected Serializer viewMetaDataSerializer;
        protected Deserializer viewMetaDataDeserializer;
        protected Condition viewMetaDataCondition;
        protected List<MetaDataColumn> viewMetaDataColumns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            String viewName = context.getConfiguration().get(PARAM_VIEW_NAME);
            viewMetaData = MetaDataSet.getDefault().getMetaData(viewName, ViewMetaData.class);

            viewMetaDataSerializer = viewMetaData.getSerializer();
            viewMetaDataDeserializer = viewMetaData.getDeserializer();
            viewMetaDataCondition = viewMetaData.getCondition();
            viewMetaDataColumns = viewMetaData.getMetaDataColumns();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
