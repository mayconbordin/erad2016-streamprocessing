package br.ufrgs.inf.gppd.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TextFilterFunction
    implements FlatMapFunction<Tuple2<Long, String>, Tuple2<Long, String>>
{
    private static final long serialVersionUID = 42l;

    @Override
    public void flatMap(Tuple2<Long,String> tweet,Collector<Tuple2<Long, String>> out) throws Exception
    {
        String text = tweet.f1;
        text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
        out.collect(new Tuple2<>(tweet.f0, text));
    }

}
