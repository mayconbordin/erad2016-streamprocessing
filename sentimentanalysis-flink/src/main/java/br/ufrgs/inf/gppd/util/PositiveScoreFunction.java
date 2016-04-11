package br.ufrgs.inf.gppd.util;

import br.ufrgs.inf.gppd.nlp.PositiveWords;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Set;

public class PositiveScoreFunction
        implements FlatMapFunction<Tuple2<Long, String>, Tuple3<Long, String, Float>>

{
    private static final long serialVersionUID = 42l;


    @Override
    public void flatMap(Tuple2<Long, String> tweet, Collector<Tuple3<Long, String, Float>> out) throws Exception {

        String text = tweet.f1;
        Set<String> posWords = PositiveWords.getWords();
        String[] words = text.split(" ");
        int numWords = words.length;
        int numPosWords = 0;
        for (String word : words) {
            if (posWords.contains(word))
                numPosWords++;
        }

        out.collect(new Tuple3<>(
                tweet.f0,
                tweet.f1,
                (float) numPosWords / numWords));

    }
}