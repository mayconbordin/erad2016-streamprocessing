package br.ufrgs.inf.gppd.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class ScoreTweetsFunction
    implements FlatMapFunction<Tuple4<Long, String, Float, Float>,
                            Tuple5<Long, String, Float, Float, String>>
{
    private static final long serialVersionUID = 42l;


    @Override
    public void flatMap(Tuple4<Long, String, Float, Float> tweet, Collector<Tuple5<Long, String, Float, Float, String>> out) throws Exception {
        String score;
        if (tweet.f2 >= tweet.f3)
            score = "positive";
        else
            score = "negative";
        out.collect(new Tuple5<>(
                tweet.f0,
                tweet.f1,
                tweet.f2,
                tweet.f3,
                score));
    }
}
