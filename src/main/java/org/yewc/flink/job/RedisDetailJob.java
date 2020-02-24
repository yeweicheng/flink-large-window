package org.yewc.flink.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.executor.base.jar.BrsExecutionDataFlow;
import com.flink.executor.base.jar.IBrsExecutionEnvironment;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.RowKeySelector;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.yewc.flink.function.DetailRedisFunction;

import java.util.Map;

/**
 * 自定义的SDK，仅参考，学习者应该忽略这个类
 */
public class RedisDetailJob implements IBrsExecutionEnvironment {

    private StreamTableEnvironment streamTableEnvironment;
    private BrsExecutionDataFlow brsExecutionDataFlow;
    private String uid;

    @Override
    public void brsExecutionEnvironment(StreamTableEnvironment streamTableEnvironment, Map<String, String> params,
                                        BrsExecutionDataFlow brsExecutionDataFlow) {

        this.streamTableEnvironment = streamTableEnvironment;
        this.brsExecutionDataFlow = brsExecutionDataFlow;

        StreamQueryConfig qConfig = streamTableEnvironment.queryConfig();
        if (params.containsKey("min.idle.state") && params.containsKey("max.idle.state")) {
            String minIdleStateRetention = params.get("min.idle.state");
            String maxIdleStateRetention = params.get("max.idle.state");
            qConfig.withIdleStateRetentionTime(Time.seconds(Integer.parseInt(minIdleStateRetention)),
                    Time.seconds(Integer.parseInt((maxIdleStateRetention))));
        }

        // 输入数据的Table和来源类型
        final String inputTableName = params.get("input.table.name");
        final String inputTableType = params.getOrDefault("input.table.type", "source");

        // 输出数据的Table和来源类型
        final String outputTableName = params.get("output.table.name");
        final String outputTableType = params.getOrDefault("output.table.type", "sink");

        // processor uid标识
        this.uid = params.getOrDefault("uid", "default");

        // h key的前缀，可以动态，如field_0
        final String hkeyPrefix = params.getOrDefault("hkey.prefix", "");

        // h key的后缀，可以动态，如field_0
        final String hkeySuffix = params.getOrDefault("hkey.suffix", "");

        // h key的field，如0
        final int hkeyField = Integer.valueOf(params.get("hkey.field"));

        // h key的value下标，如0,1,2
        final String hkeyValues = params.getOrDefault("hkey.values", "");

        // 详单分割字符
        final String splitDetailChar = params.getOrDefault("split.detail.char", ",");

        // 检查field是否已经存在
        final boolean checkExists = Boolean.valueOf(params.getOrDefault("check.exists", "false"));

        // 打印结果
        final boolean print = Boolean.valueOf(params.getOrDefault("print", "false"));

        // redis相关
        final String redisAddress = params.get("redis.address");
        final Integer redisPort = Integer.valueOf(params.get("redis.port"));
        final String redisPasswd = params.get("redis.passwd");
        final Integer redisExpire = Integer.valueOf(params.get("redis.expire"));

        // 输入源
        DataStream sourceDataStream = getInputTable(inputTableName, inputTableType, qConfig);

        ProcessFunction processFunction
                = DetailRedisFunction.getInstance(hkeyPrefix, hkeySuffix, hkeyField, hkeyValues, splitDetailChar)
                .setRedisUtil(redisAddress, redisPort, redisPasswd, redisExpire, checkExists)
                .isPrint(print);

        // 执行
        SingleOutputStreamOperator<Tuple2> windowStream = sourceDataStream
                .map((v) -> v instanceof Tuple2 ? ((Tuple2) v).f1 : v) // 中间表的情况
                .name("map_name_" + uid).uid("map_uid_" + uid)
                .process(processFunction)
                .name("process_name_" + uid).uid("process_uid_" + uid)
                .returns(Types.TUPLE(Types.BOOLEAN, Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING)));

        // 输出源
        StreamTableSink tableSink = ((StreamTableSink) brsExecutionDataFlow.getTableSinkMap().get(outputTableName));
        tableSink.emitDataStream(windowStream);
    }

    private DataStream getInputTable(String tableName, String tableType, StreamQueryConfig qConfig) {
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
            return streamTableEnvironment.toRetractStream(dataSource, Types.ROW(typeArr), qConfig);
        }
        return null;
    }
}
