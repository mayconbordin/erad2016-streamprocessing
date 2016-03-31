package br.ufrgs.inf.gppd.function;

import br.ufrgs.inf.gppd.nlp.StopWords;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.List;

public class StemmingFunction
    implements Function<Tuple2<Long, String>, Tuple2<Long, String>>
{
    private static final long serialVersionUID = 42l;

    public Tuple2<Long, String> call(Tuple2<Long, String> tweet)
    {
        String text = tweet._2();
        List<String> stopWords = StopWords.getWords();
        for (String word : stopWords)
        {
            text = text.replaceAll("\\b" + word + "\\b", "");
        }
        return new Tuple2<Long, String>(tweet._1(), text);
    }
}
