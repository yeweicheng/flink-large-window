package org.yewc.test;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.runtime.RowKeySelector;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.yewc.flink.function.CenterFunction;
import org.yewc.flink.watermark.EventWatermark;

import java.util.Properties;

public class DataStreamTest {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.16.6.191:9092");
        properties.setProperty("zookeeper.connect", "10.16.6.185:2181");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer<>("test2", new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(myConsumer);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final Long windowSize = 5*60000L;
        final Long slideSize = 60000L;

        final int timeField = 2;
        final String groupString = "{\"group\": [\"distinct_1\", \"count_1\"]}";

        final boolean keepOldData = false;

        int[] keys = {0};
        final KeySelector keySelector = new RowKeySelector(keys, TypeInformation.of(Row.class));

        DataStream counts =
                stream.flatMap(new Tokenizer())
                        .assignTimestampsAndWatermarks(new EventWatermark(timeField))
                        .keyBy(keySelector)
                        .process(new CenterFunction(keepOldData, windowSize, slideSize, timeField, groupString));


        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        // execute program
        env.execute("WindowWordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Row> {

        @Override
        public void flatMap(String value, Collector<Row> out) {
            try {
                JSONObject temp = JSONObject.parseObject(value);
                Row row = new Row(3);
                row.setField(0, temp.getString("c1"));
                row.setField(1, temp.getInteger("c2"));
                row.setField(2, temp.getString("c3"));
                out.collect(row);
            } catch (Exception e) {
                if (!(e instanceof JSONException)) {
                    throw e;
                }
            }

        }
    }


}
