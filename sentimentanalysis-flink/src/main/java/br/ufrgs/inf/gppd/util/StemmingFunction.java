package br.ufrgs.inf.gppd.util;

import br.ufrgs.inf.gppd.nlp.StopWords;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public class StemmingFunction
    implements FlatMapFunction<Tuple2<Long, String>, Tuple2<Long, String>>
{
    private static final long serialVersionUID = 42l;

    public void flatMap(Tuple2<Long,String> tweet,Collector<Tuple2<Long, String>> out) throws Exception {
        String text = tweet.f1;
        List<String> stopWords = StopWords.getWords();
        for (String word : stopWords) {
            text = text.replaceAll("\\b" + word + "\\b", "");
        }

        out.collect(new Tuple2<>(tweet.f0, text));


    }
}
