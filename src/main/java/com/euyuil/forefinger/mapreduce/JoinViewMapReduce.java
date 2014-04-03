package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.view.JoinViewMetaData;
import com.euyuil.forefinger.meta.view.ViewMetaDataColumn;
import com.euyuil.forefinger.serde.ArrayDataRow;
import com.euyuil.forefinger.serde.DataRow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.27
 */
public class JoinViewMapReduce {

    public static final String PARAM_DATA_NAME = "dataName";

    /**
     * Set a JoinViewMetaData as input.
     * Maps (offset, row) to (joints, row)
     */
    public static class JoinViewMapper
            extends ViewMapReduce.ViewMapper<LongWritable, Text, CompositeKeyWritable, CompositeValueWritable> {

        /**
         * The join view of this mapper.
         */
        protected JoinViewMetaData joinViewMetaData;

        /**
         * The current join source of this mapper.
         * Join source is one of the source views of the join view.
         * This variable contains the name of the source view, and the join type (inner or outer).
         */
        protected JoinViewMetaData.JoinSource joinSource;

        /**
         * The ID (i.e. the order) of the join source in the join source list of the join view.
         */
        protected int joinSourceId;

        /**
         * The joints of the source join views.
         * If all of the values of these columns are the same, the entries will be joined.
         */
        protected List<JoinViewMetaData.JoinItem> joinItems;

        /**
         * This value is for current join source.
         * The column IDs corresponding to the joinItems.
         * For optimization.
         */
        protected int[] joinItemColumnIds;

        /**
         * These values are the values that the current join source should provide.
         * i.e. they are selected as projection parameters.
         */
        protected ArrayList<Integer> selectedColumnIds;

        /**
         * Some cache.
         */
        private CompositeKeyWritable compositeKeyWritable = new CompositeKeyWritable();
        private CompositeValueWritable compositeValueWritable = new CompositeValueWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);

            if (!(viewMetaData instanceof JoinViewMetaData))
                throw new IOException("SimpleViewMetaData expected");
            joinViewMetaData = (JoinViewMetaData) viewMetaData;

            String sourceDataName = context.getConfiguration().get(PARAM_DATA_NAME);
            List<JoinViewMetaData.JoinSource> sources = joinViewMetaData.getSources();
            for (int i = 0; i < sources.size(); i++) {
                JoinViewMetaData.JoinSource source = sources.get(i);
                if (source.getDataName().equals(sourceDataName)) {
                    joinSource = source;
                    joinSourceId = i;
                    break;
                }
            }

            joinItems = joinViewMetaData.getJoinItems();

            joinItemColumnIds = new int[joinItems.size()];
            for (int i = 0; i < joinItemColumnIds.length; ++i) {
                JoinViewMetaData.JoinItem joinItem = joinItems.get(i);
                for (int j = 0; j < joinItem.getColumns().size(); ++j) {
                    JoinViewMetaData.JoinItem.Column column = joinItem.getColumns().get(j);
                    if (column.getDataName().equals(joinSource.getDataName())) {
                        joinItemColumnIds[i] = j;
                        break;
                    }
                }
            }

            compositeKeyWritable.resize(joinItems.size());

            // boolean inputIsDefault = joinViewMetaData.getSources().get(0).getDataName().equals(joinSource.getDataName());

            for (int i = 0; i < joinViewMetaData.getMetaDataColumns().size(); ++i) {
                ViewMetaDataColumn column = (ViewMetaDataColumn) joinViewMetaData.getMetaDataColumns().get(i);
                if (column.getSourceDataName().equals(joinSource.getDataName()))
                    selectedColumnIds.add(i);
            }

            compositeValueWritable.resize(selectedColumnIds.size() + 1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            DataRow dataRow = viewMetaDataDeserializer.deserialize(value.toString());

            // Filter conditions.
            if (viewMetaDataCondition != null && !viewMetaDataCondition.fulfilled(dataRow))
                return;

            // Joints, i.e. joinItems.
            for (int i = 0; i < joinItemColumnIds.length; ++i)
                compositeKeyWritable.setObject(i, dataRow.get(joinItemColumnIds[i]));

            // Projections.
            compositeValueWritable.setObject(0, joinSourceId);
            for (int i = 0; i < selectedColumnIds.size(); ++i)
                compositeValueWritable.setObject(i + 1, dataRow.get(selectedColumnIds.get(i)));

            // Write results.
            context.write(compositeKeyWritable, compositeValueWritable);
        }
    }

    public static class JoinViewReducer
            extends ViewMapReduce.ViewReducer<CompositeKeyWritable, CompositeValueWritable, NullWritable, Text> {

        /**
         * The join view of this mapper.
         */
        protected JoinViewMetaData joinViewMetaData;

        protected List<JoinViewMetaData.JoinSource> joinSources;

        /**
         * The joints of the source join views.
         * If all of the values of these columns are the same, the entries will be joined.
         */
        protected List<JoinViewMetaData.JoinItem> joinItems;

        /**
         * These values are the values that the current join source should provide.
         * i.e. they are selected as projection parameters.
         */
        protected ArrayList<Integer>[] selectedColumnIds;

        @Override
        @SuppressWarnings("unchecked")
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);

            if (!(viewMetaData instanceof JoinViewMetaData))
                throw new IOException("SimpleViewMetaData expected");
            joinViewMetaData = (JoinViewMetaData) viewMetaData;

            joinSources = joinViewMetaData.getSources();
            joinItems = joinViewMetaData.getJoinItems();

            selectedColumnIds = new ArrayList[joinSources.size()];

            for (int i = 0; i < joinViewMetaData.getMetaDataColumns().size(); ++i) {
                ViewMetaDataColumn column = (ViewMetaDataColumn) joinViewMetaData.getMetaDataColumns().get(i);
                for (int j = 0; j < joinSources.size(); ++j) {
                    JoinViewMetaData.JoinSource joinSource = joinSources.get(j);
                    if (column.getSourceDataName().equals(joinSource.getDataName()))
                        selectedColumnIds[j].add(i);
                }
            }
        }

        @Override
        protected void reduce(CompositeKeyWritable key, Iterable<CompositeValueWritable> values, Context context) throws IOException, InterruptedException {

            ArrayDataRow arrayDataRow = new ArrayDataRow(joinViewMetaData.getMetaDataColumns().size());

            for (CompositeValueWritable compositeValueWritable : values) {
                int joinSourceId = (Integer) compositeValueWritable.getObject(0);
                for (int i = 0; i < compositeValueWritable.size(); ++i)
                    arrayDataRow.set(selectedColumnIds[joinSourceId].get(i), compositeValueWritable.getObject(i));
            }

            text.set(viewMetaDataSerializer.serialize(arrayDataRow));
            context.write(NullWritable.get(), text);
        }
    }
}
