package sockets;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sockets.VilleWithPrice;

public class Sales {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;


        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", 8888, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<VilleWithPrice> windowCounts =
                text.flatMap(
                                (FlatMapFunction<String, VilleWithPrice>) (value, out) -> {
                                    String[] parts = value.split(",");
                                    if (parts.length == 3) {
                                        String date = parts[0].trim();
                                        String ville = parts[1].trim();
                                        double price = Double.parseDouble(parts[2].trim());
                                        out.collect(new VilleWithPrice(ville, price));
                                    }
                                },
                                Types.POJO(VilleWithPrice.class))
                        .keyBy(value -> value.ville)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce((a, b) -> new VilleWithPrice(a.ville, a.price + b.price))
                        .returns(VilleWithPrice.class);

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);
        System.out.println("---------------------------------------------------------");
        env.execute("Socket Window WordCount");
    }
}


