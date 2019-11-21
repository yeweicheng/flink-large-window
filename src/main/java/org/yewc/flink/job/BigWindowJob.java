package org.yewc.flink.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.executor.base.jar.BrsExecutionDataFlow;
import com.flink.executor.base.jar.IBrsExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.RowKeySelector;
import org.apache.flink.types.Row;
import org.yewc.flink.function.CenterFunction;
import org.yewc.flink.watermark.EventWatermark;

import java.util.Map;

public class BigWindowJob implements IBrsExecutionEnvironment {

    @Override
    public void brsExecutionEnvironment(StreamTableEnvironment streamTableEnvironment, Map<String, String> params,
                                        BrsExecutionDataFlow brsExecutionDataFlow) {

        final String sourceTableName = params.get("source.table");
        final String sinkTableName = params.get("sink.table");

        final Long windowSize = Long.valueOf(params.get("window.size"));
        final Long slideSize = Long.valueOf(params.get("slide.size"));

        final int timeField = Integer.valueOf(params.get("time.field"));

        final boolean keepOldData = Boolean.valueOf(params.get("keep.old.data"));

        String[] keys = params.get("key.fields").split(",");
        int[] keyIndexes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keyIndexes[i] = Integer.valueOf(keys[i]);
        }

        final String groupString = params.get("group.string");
        JSONArray groupKey = JSONObject.parseObject(groupString).getJSONArray("group");
        TypeInformation[] typeArr = new TypeInformation[groupKey.size() + 2];
        typeArr[0] = Types.STRING;
        typeArr[1] = Types.STRING;
        for (int i = 0; i < groupKey.size(); i++) {
            if (groupKey.getString(i).startsWith("sum_")) {
                typeArr[i + 2] = Types.DOUBLE;
            } else {
                typeArr[i + 2] = Types.LONG;
            }
        }

        final KeySelector keySelector = new RowKeySelector(keyIndexes, TypeInformation.of(Row.class));

        BrsExecutionDataFlow.RegistedDataSource sourceDataSource = brsExecutionDataFlow.getTableSourceMap().get(sourceTableName);
        DataStream sourceDataStream = sourceDataSource.getDataStream();

        DataStream<Row> windowStream = sourceDataStream
                .assignTimestampsAndWatermarks(new EventWatermark(timeField))
                .keyBy(keySelector)
                .process(new CenterFunction(keepOldData, windowSize, slideSize, timeField, groupString))
                .returns(Types.ROW(typeArr));

        streamTableEnvironment.registerDataStream("window_table", windowStream);
        streamTableEnvironment.sqlUpdate("insert into " + sinkTableName + " select * from window_table");
    }
}
