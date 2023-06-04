package sockets;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class wordAccount {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;


        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", 8888, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts =
                text.flatMap(
                                (FlatMapFunction<String, WordWithCount>)
                                        (value, out) -> {
                                            for (String word : value.split("\\s")) {
                                                out.collect(new WordWithCount(word, 1L));
                                            }
                                        },
                                Types.POJO(WordWithCount.class))
                        .keyBy(value -> value.word)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count))
                        .returns(WordWithCount.class);

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);
        System.out.println("---------------------------------------------------------");
        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /**
     * Data type for words with count.
     */
    public static class WordWithCount {

        public String word;
        public long count;

        @SuppressWarnings("unused")
        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {

            if (!word.equals(" "))
                return word + " : " + count;
            return null;
        }
    }
}