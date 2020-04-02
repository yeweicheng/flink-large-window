package org.yewc.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.RowKeySelector;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.yewc.flink.function.GlobalFunction;
import org.yewc.flink.watermark.TheWatermark;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class DataStreamTest {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStateBackend(new RocksDBStateBackend("hdfs://10.16.6.185:8020/flink/flink-rocksdb"));
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.16.6.191:9092");
        properties.setProperty("zookeeper.connect", "10.16.6.185:2181");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer<>("test2", new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(myConsumer);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final Long windowSize = 86400000L + 3600000L;
        final Long slideSize = 60000L;
        final Long lateness = 5000L;

        final int timeField = 4;
        final String groupString = "{\"field\": [\"key_0\", \"endtime\"], " +
                "\"group\": [\"distinct_1\", \"distinct_long_2\", \"distinct_int_1\", \"count_1\"]}";

        int[] keys = {0};
        final KeySelector keySelector = new RowKeySelector(keys, TypeInformation.of(Row.class));

        // 测试中间层
        DataStream buffer = stream.flatMap(new Tokenizer()).returns(Types.ROW(Types.STRING, Types.INT, Types.LONG, Types.STRING, Types.SQL_TIMESTAMP));
        ste.registerDataStream("source_table", buffer, "c1,c2,c3,c4,c5");
        Table table = ste.sqlQuery("select * from source_table");
        DataStream midle = ste.toRetractStream(table, table.getSchema().toRowType());

        GlobalFunction gf = GlobalFunction.getInstance()
                .setKeepOldData(false)
                .setWindowSplit(slideSize, windowSize)
                .setLateness(lateness)
                .setTimeField(timeField)
                .setGroupSchema(groupString)
                .setAlwaysCalculate(false)
                .setStartZeroTime(true)
                .setRecountLateData(true)
                .setOnlyLastOneWindow(true)
                .setKeepLateZeroTime(86400000L);

        DataStream counts = midle.map((v) -> {
                                if (v instanceof Tuple2) {
                                    return ((Tuple2) v).f1;
                                }
                                return v;
                        })
                        .assignTimestampsAndWatermarks(new TheWatermark(timeField, env.getStreamTimeCharacteristic()))
                        .keyBy(keySelector)
                        .process(gf)
                        .returns(Types.TUPLE(Types.BOOLEAN, Types.ROW(Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.LONG)));

        counts.print();
        env.execute("WindowWordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Row> {

        @Override
        public void flatMap(String value, Collector<Row> out) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                JSONObject temp = JSONObject.parseObject(value);
                Row row = new Row(5);
                row.setField(0, temp.getString("c1"));
                row.setField(1, temp.getInteger("c2"));
                row.setField(2, temp.getLong("c3"));
                row.setField(3, temp.getString("c4"));
                if (temp.getString("c5").length() == 19) {
                    row.setField(4, new Timestamp(sdf.parse(temp.getString("c5")).getTime()));
                } else {
                    row.setField(4, new Timestamp(temp.getLong("c5")));
                }

                out.collect(row);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


}
