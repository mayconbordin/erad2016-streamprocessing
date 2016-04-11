package br.ufrgs.inf.gppd.util;

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple3;
import scala.Tuple4;

public class TestCoMap
        implements CoFlatMapFunction<Tuple3<Long, String, Float>,Tuple3<Long, String, Float>, Tuple4<Long, String, Float, Float>> {

    @Override
    public void flatMap1(Tuple3<Long, String, Float> positive, Collector<Tuple4<Long, String, Float, Float>> out) throws Exception {
        out.collect(new Tuple4<>(positive._1(),
                                 positive._2(),
                                 positive._3(),
                                 (float)0));
    }

    @Override
    public void flatMap2(Tuple3<Long, String, Float> negative, Collector<Tuple4<Long, String, Float, Float>> out) throws Exception {
        out.collect(new Tuple4<>(negative._1(),
                negative._2(),
                (float) 0,
                negative._3()));
    }
}