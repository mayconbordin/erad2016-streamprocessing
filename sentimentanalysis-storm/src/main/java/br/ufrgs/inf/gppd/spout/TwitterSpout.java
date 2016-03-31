package br.ufrgs.inf.gppd.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author davidk
 */
public class TwitterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    private TwitterStream twitterStream;

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<String>(1000);
        this.collector = collector;

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                queue.offer(TwitterObjectFactory.getRawJSON(status));
            }

            public void onDeletionNotice(StatusDeletionNotice sdn) { }
            public void onTrackLimitationNotice(int i) { }
            public void onScrubGeo(long l, long l1) { }
            public void onStallWarning(StallWarning stallWarning) { }
            public void onException(Exception e) { }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);

        TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
        twitterStream = factory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.filter(new FilterQuery().language("en").track("trump"));
    }

    public void nextTuple() {
        String ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}