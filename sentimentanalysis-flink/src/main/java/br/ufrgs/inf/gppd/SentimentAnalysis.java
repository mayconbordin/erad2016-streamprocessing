package br.ufrgs.inf.gppd;


import br.ufrgs.inf.gppd.util.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


public class SentimentAnalysis {

        public static void main(String[] args) throws Exception {

            // Checking input parameters
            final ParameterTool params = ParameterTool.fromArgs(args);

            // set up the execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // make parameters available in the web interface
            env.getConfig().setGlobalJobParameters(params);

            env.setParallelism(params.getInt("parallelism", 1));

            DataStream<String> streamSource = env.fromElements(TwitterExampleData.TEXTS);

            DataStream<Tuple2<Long, String>> tweets = streamSource
                    .flatMap(new TwitterFilterFunction());

            DataStream<Tuple2<Long, String>> filtered = tweets.filter(
                    tweet -> tweet != null
            );

            DataStream<Tuple2<Long, String>> tweetsFiltered = filtered
                    .flatMap(new TextFilterFunction());

            tweetsFiltered = tweetsFiltered
                    .flatMap(new StemmingFunction());

            DataStream<Tuple3<Long, String, Float>> positiveTweets =
                    tweetsFiltered.flatMap(new PositiveScoreFunction());

            DataStream<Tuple3<Long, String, Float>> negativeTweets =
                    tweetsFiltered.flatMap(new NegativeScoreFunction());

            DataStream<Tuple4<Long, String, Float, Float>> scoredTweets = positiveTweets
                    .join(negativeTweets)
                    .onWindow(10, TimeUnit.SECONDS)
                    .where(0,1)
                    .equalTo(0,1)
                    .with(new JoinFunction<Tuple3<Long, String, Float>, Tuple3<Long, String, Float>, Tuple4<Long, String, Float, Float>>() {
                        @Override
                        public Tuple4<Long, String, Float, Float> join(Tuple3<Long, String, Float> positive, Tuple3<Long, String, Float> negative) throws Exception {
                            return new Tuple4<>(positive.f0, positive.f1, positive.f2,negative.f2);
                        }
                    });

            DataStream<Tuple5<Long, String, Float, Float, String>> result =
                    scoredTweets.flatMap(new ScoreTweetsFunction());

            result.print();

            result.writeAsText("file:///home/veith/erad2016-streamprocessing/results/teste-flink");

            env.execute("Twitter Streaming Example");
        }

    }