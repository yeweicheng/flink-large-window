package org.yewc.flink.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.executor.base.jar.BrsExecutionDataFlow;
import com.flink.executor.base.jar.IBrsExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.RowKeySelector;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.yewc.flink.function.CenterFunction;
import org.yewc.flink.watermark.EventWatermark;

import java.util.Map;

/**
 * 自定义的SDK，仅参考，学习者应该忽略这个类
 */
public class BigWindowJob implements IBrsExecutionEnvironment {

    private StreamTableEnvironment streamTableEnvironment;
    private BrsExecutionDataFlow brsExecutionDataFlow;

    @Override
    public void brsExecutionEnvironment(StreamTableEnvironment streamTableEnvironment, Map<String, String> params,
                                        BrsExecutionDataFlow brsExecutionDataFlow) {

        this.streamTableEnvironment = streamTableEnvironment;
        this.brsExecutionDataFlow = brsExecutionDataFlow;

        final String inputTableName = params.get("input.table.name");
        final String inputTableType = params.getOrDefault("input.table.type", "source");
        final String outputTableName = params.get("output.table.name");
        final String outputTableType = params.getOrDefault("output.table.type", "sink");

        final Long windowSize = Long.valueOf(params.get("window.size"));
        final Long slideSize = Long.valueOf(params.get("slide.size"));
        if (slideSize > windowSize) {
            throw new RuntimeException("invalid sliding time and window time");
        }
        final Long lateness = Long.valueOf(params.getOrDefault("lateness", "0"));

        final int timeField = Integer.valueOf(params.get("time.field"));

        final boolean keepOldData = Boolean.valueOf(params.getOrDefault("keep.old.data", "false"));

        // 字段类型返回
        final String groupString = params.get("group.string");


        // 选择 key
        String[] keys = params.get("key.fields").split(",");
        int[] keyIndexes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keyIndexes[i] = Integer.valueOf(keys[i]);
        }
        final KeySelector keySelector = new RowKeySelector(keyIndexes, TypeInformation.of(Row.class));

        DataStream sourceDataStream = getInputTable(inputTableName, inputTableType);

        // 执行
        DataStream<Row> windowStream = sourceDataStream
                .map((v) -> v instanceof Tuple2 ? ((Tuple2) v).f1 : v) // 中间表的情况
                .assignTimestampsAndWatermarks(new EventWatermark(timeField))
                .keyBy(keySelector)
                .process(new CenterFunction(keepOldData, windowSize, slideSize, lateness, timeField, groupString))
                .returns(Types.ROW(getReturnTypes(groupString, inputTableName, inputTableType)));

        toOutputTable(windowStream, outputTableName, outputTableType, String.join(",", getReturnFields(groupString)));
    }

    private DataStream getInputTable(String tableName, String tableType) {
        if ("source".equals(tableType)) {
            BrsExecutionDataFlow.RegistedDataSource dataSource = brsExecutionDataFlow.getTableSourceMap().get(tableName);
            return dataSource.getDataStream();
        } else if ("buffer".equals(tableType)) {
            Table dataSource = brsExecutionDataFlow.getBufferTableMap().get(tableName);
            TypeInformation[] typeTemp = dataSource.getSchema().getFieldTypes();
            TypeInformation[] typeArr = new TypeInformation[typeTemp.length];
            for (int i = 0; i < typeTemp.length; i++) {
                typeArr[i] = typeTemp[i];
                if (typeArr[i] instanceof TimeIndicatorTypeInfo) {
                    typeArr[i] = Types.SQL_TIMESTAMP;
                }
            }
            return streamTableEnvironment.toRetractStream(dataSource, Types.ROW(typeArr));
        }
        return null;
    }

    private TypeInformation getInputTableField(String tableName, String tableType, int fieldIndex) {
        if ("source".equals(tableType)) {
            BrsExecutionDataFlow.RegistedDataSource dataSource = brsExecutionDataFlow.getTableSourceMap().get(tableName);
            return dataSource.getTable().getSchema().getFieldType(fieldIndex).get();
        } else if ("buffer".equals(tableType)) {
            Table dataSource = brsExecutionDataFlow.getBufferTableMap().get(tableName);
            TypeInformation type = dataSource.getSchema().getFieldType(fieldIndex).get();
            if (type instanceof TimeIndicatorTypeInfo) {
                type = Types.SQL_TIMESTAMP;
            }
            return type;
        }
        return Types.STRING;
    }

    private void toOutputTable(DataStream<Row> windowStream,
                               String tableName, String tableType, String fields) {
        if ("sink".equals(tableType)) {
            streamTableEnvironment.registerDataStream("window_table", windowStream);
            streamTableEnvironment.sqlUpdate("insert into " + tableName + " select * from window_table");
        } else if ("buffer".equals(tableType)) {
            streamTableEnvironment.registerDataStream(tableName, windowStream, fields);
        }
    }

    private TypeInformation[] getReturnTypes(String groupString, String tableName, String tableType) {
        final JSONObject groupJson = JSONObject.parseObject(groupString);
        JSONArray fieldKey = groupJson.getJSONArray("field");
        JSONArray groupKey = groupJson.getJSONArray("group");
        int groupSize = groupKey.size();
        int fieldSize = fieldKey.size();
        TypeInformation[] typeArr = new TypeInformation[fieldSize + groupSize];
        for (int i = 0; i < fieldSize; i++) {
            String temp = fieldKey.getString(i);
            if (temp.startsWith("starttime") || temp.startsWith("endtime")) {
                if (temp.endsWith("_10") || temp.endsWith("_13")) {
                    typeArr[i] = Types.LONG;
                } else {
                    typeArr[i] = Types.STRING;
                }
            } else if (temp.startsWith("key")) {
                String[] keySplit = temp.split("_");
                if (keySplit.length == 1) {
                    typeArr[i] = Types.STRING;
                } else {
                    typeArr[i] = getInputTableField(tableName, tableType, Integer.valueOf(keySplit[1]));
                }
            }
        }
        for (int i = 0; i < groupSize; i++) {
            String temp = groupKey.getString(i);
            if (temp.startsWith("sum_")) {
                typeArr[i + fieldSize] = Types.DOUBLE;
            } else {
                typeArr[i + fieldSize] = Types.LONG;
            }
        }
        return typeArr;
    }

    private String[] getReturnFields(String groupString) {
        final JSONObject groupJson = JSONObject.parseObject(groupString);
        JSONArray fieldKey = groupJson.getJSONArray("field");
        JSONArray groupKey = groupJson.getJSONArray("group");
        int groupSize = groupKey.size();
        int fieldSize = fieldKey.size();
        String[] fieldArr = new String[fieldSize + groupSize];
        for (int i = 0; i < fieldSize; i++) {
            fieldArr[i] = fieldKey.getString(i);
        }
        for (int i = 0; i < groupSize; i++) {
            fieldArr[i + fieldSize] = groupKey.getString(i);
        }
        return fieldArr;
    }
}
