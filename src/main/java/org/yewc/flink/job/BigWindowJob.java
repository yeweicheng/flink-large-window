package org.yewc.flink.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.executor.base.jar.BrsExecutionDataFlow;
import com.flink.executor.base.jar.IBrsExecutionEnvironment;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.RowKeySelector;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.yewc.flink.function.CenterFunction;
import org.yewc.flink.function.GlobalFunction;
import org.yewc.flink.watermark.TheWatermark;

import java.io.IOException;
import java.util.Map;

/**
 * 自定义的SDK，仅参考，学习者应该忽略这个类
 */
public class BigWindowJob implements IBrsExecutionEnvironment {

    private StreamTableEnvironment streamTableEnvironment;
    private BrsExecutionDataFlow brsExecutionDataFlow;
    private String uid;

    @Override
    public void brsExecutionEnvironment(StreamTableEnvironment streamTableEnvironment, Map<String, String> params,
                                        BrsExecutionDataFlow brsExecutionDataFlow) {

        this.streamTableEnvironment = streamTableEnvironment;
        this.brsExecutionDataFlow = brsExecutionDataFlow;

//        StateBackend sb = this.streamTableEnvironment.execEnv().getStateBackend();
//        if (sb instanceof RocksDBStateBackend) {
//            RocksDBStateBackend rocksdb = (RocksDBStateBackend) sb;
//            DefaultConfigurableOptionsFactory op = ((DefaultConfigurableOptionsFactory) rocksdb.getOptions());
//            if (op == null) {
//                op = new DefaultConfigurableOptionsFactory();
//            }
//            op.setTargetFileSizeBase("256MB");
//            op.setWriteBufferSize("256MB");
//            op.setMaxBackgroundThreads(4);
//            rocksdb.setOptions(op);
//        }

        // 输入数据的Table和来源类型
        final String inputTableName = params.get("input.table.name");
        final String inputTableType = params.getOrDefault("input.table.type", "source");

        // 输出数据的Table和来源类型
        final String outputTableName = params.get("output.table.name");
        final String outputTableType = params.getOrDefault("output.table.type", "sink");

        // 窗口大小和滑动大小
        final Long windowSize = Long.valueOf(params.get("window.size"));
        final Long slideSize = Long.valueOf(params.get("slide.size"));
        if (slideSize > windowSize) {
            throw new RuntimeException("invalid sliding time and window time");
        }

        // 延迟触发时长
        final Long lateness = Long.valueOf(params.getOrDefault("lateness", "0"));

        // watermark选用的字段
        final int timeField = Integer.valueOf(params.get("time.field"));

        // 是否保留旧数据输出
        final boolean keepOldData = Boolean.valueOf(params.getOrDefault("keep.old.data", "false"));

        // 不管是否有新数据都输出
        final boolean alwaysCalculate = Boolean.valueOf(params.getOrDefault("always.calculate", "true"));

        // 是否从0点开始算，一般用于天累计
        final boolean startZeroTime = Boolean.valueOf(params.getOrDefault("start.zero.time", "false"));

        // 迟到数据是否重算它当前及之后的窗口
        final boolean recountLateData = Boolean.valueOf(params.getOrDefault("recount.late.data", "false"));

        // 批量模式
        final boolean batch = Boolean.valueOf(params.getOrDefault("batch.mode", "false"));

        // 条件过滤
        final String where = params.getOrDefault("where", "");

        // 查询过滤
        final String select = params.getOrDefault("select", "*");

        // 字段类型返回
        final String groupString = params.get("group.string");

        // processor uid标识
        this.uid = params.getOrDefault("uid", "default");

        // 时间驱动
        TimeCharacteristic tc = streamTableEnvironment.execEnv().getStreamTimeCharacteristic();
        if (TimeCharacteristic.IngestionTime.equals(tc)) {
            throw new RuntimeException("unsupported ingestion time");
        }

        // 选择 key
        String[] keys = params.get("key.fields").split(",");
        int[] keyIndexes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keyIndexes[i] = Integer.valueOf(keys[i]);
        }
        final KeySelector keySelector = new RowKeySelector(keyIndexes, TypeInformation.of(Row.class));

        final String executeMode = params.getOrDefault("processor.mode", "global");
        KeyedProcessFunction processFunction;
        if ("center".equals(executeMode)) {
            processFunction = new CenterFunction(keepOldData, windowSize, slideSize, lateness, timeField, groupString);
        } else {
            processFunction = GlobalFunction.getInstance()
                    .setKeepOldData(keepOldData)
                    .setWindowSplit(slideSize, windowSize)
                    .setLateness(lateness)
                    .setTimeField(timeField)
                    .setGroupSchema(groupString)
                    .setBatch(batch)
                    .setAlwaysCalculate(alwaysCalculate)
                    .setStartZeroTime(startZeroTime)
                    .setRecountLateData(recountLateData);
        }

        TypeInformation rowTypes = Types.ROW(getReturnTypes(groupString, inputTableName, inputTableType));

        // 输入源
        DataStream sourceDataStream = getInputTable(inputTableName, inputTableType);

        // 执行
        SingleOutputStreamOperator<Tuple2> windowStream = sourceDataStream
                .map((v) -> v instanceof Tuple2 ? ((Tuple2) v).f1 : v) // 中间表的情况
                .name("map_name_" + uid).uid("map_uid_" + uid)
                .assignTimestampsAndWatermarks(new TheWatermark(timeField, tc))
                .keyBy(keySelector)
                .process(processFunction)
                .name("process_name_" + uid).uid("process_uid_" + uid)
                .returns(Types.TUPLE(Types.BOOLEAN, rowTypes));
//        if (params.containsKey("uid")) {
//            windowStream = windowStream.uid(params.get("uid"));
//        }

        // 输出源
        toOutputTable(windowStream, outputTableName, outputTableType, select, where, rowTypes,
                String.join(",", getReturnFields(groupString)));
    }

    private DataStream getInputTable(String tableName, String tableType) {
        if ("source".equals(tableType)) {
            BrsExecutionDataFlow.RegistedDataSource dataSource = brsExecutionDataFlow.getTableSourceMap().get(tableName);
            return ((SingleOutputStreamOperator) dataSource.getDataStream())
                    .name("source_table_name_" + uid).uid("source_table_uid_" + uid);
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

    private void toOutputTable(DataStream<Tuple2> windowStream, String tableName, String tableType,
                               String select, String where, TypeInformation rowTypes, String fields) {
        if (!"*".equals(select) || StringUtils.isNotBlank(where)) {
            streamTableEnvironment.registerDataStream(tableName + "_" + uid,
                    windowStream.map((v) -> v.f1).returns(rowTypes),
                    fields);
            String sql = " select " + select + " from " + tableName + "_" + uid + " where 1 = 1 and " + where;

            if ("sink".equals(tableType)) {
                streamTableEnvironment.sqlUpdate(" insert into " + tableName + sql);
            } else if ("buffer".equals(tableType)) {
                Table table = streamTableEnvironment.sqlQuery(sql);
                streamTableEnvironment.registerTable(tableName, table);
            }
        } else {
            if ("sink".equals(tableType)) {
                StreamTableSink tableSink = ((StreamTableSink) brsExecutionDataFlow.getTableSinkMap().get(tableName));
                tableSink.emitDataStream(windowStream);
            } else if ("buffer".equals(tableType)) {
                streamTableEnvironment.registerDataStream(tableName, windowStream, fields);
            }
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
