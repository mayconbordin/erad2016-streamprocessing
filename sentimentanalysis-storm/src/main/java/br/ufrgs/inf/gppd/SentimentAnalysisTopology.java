package br.ufrgs.inf.gppd;

import br.ufrgs.inf.gppd.bolt.*;
import br.ufrgs.inf.gppd.spout.TwitterSpout;
import br.ufrgs.inf.gppd.utils.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.*;

import java.util.UUID;

public class SentimentAnalysisTopology {
    private final Logger logger = Logger.getLogger(this.getClass());

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], createConfig(false), createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("sentiment-analysis", createConfig(true), createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }

    private static KafkaSpout createKafkaSpout() {
        String zkConnString = Properties.getString("sa.storm.zkhosts");
        String topicName = Properties.getString("sa.storm.kafka_topic");

        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }

    private static HdfsBolt createHdfsBolt() {
        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(Properties.getString("sa.storm.hdfs_output_file"));

        return new HdfsBolt()
                .withFsUrl(Properties.getString("sa.storm.hdfs_url"))
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
    }

    private static StormTopology createTopology() {
        TopologyBuilder topology = new TopologyBuilder();

        topology.setSpout("twitter_spout", new TwitterSpout());

        /*topology.setBolt("print", new PrinterBolt(), 4)
                .shuffleGrouping("twitter_spout");*/


        //topology.setSpout("twitter_spout", createKafkaSpout(), 4);

        topology.setBolt("twitter_filter", new TwitterFilterBolt(), 4)
                .shuffleGrouping("twitter_spout");

        topology.setBolt("text_filter", new TextFilterBolt(), 4)
                .shuffleGrouping("twitter_filter");

        topology.setBolt("stemming", new StemmingBolt(), 4)
                .shuffleGrouping("text_filter");

        topology.setBolt("positive", new PositiveSentimentBolt(), 4)
                .shuffleGrouping("stemming");
        topology.setBolt("negative", new NegativeSentimentBolt(), 4)
                .shuffleGrouping("stemming");

        topology.setBolt("join", new JoinSentimentsBolt(), 4)
                .fieldsGrouping("positive", new Fields("tweet_id"))
                .fieldsGrouping("negative", new Fields("tweet_id"));

        topology.setBolt("score", new SentimentScoringBolt(), 4)
                .shuffleGrouping("join");

        topology.setBolt("print", new PrinterBolt(), 4)
                .shuffleGrouping("score");

        topology.setBolt("csv", new CsvBolt(), 4)
                .shuffleGrouping("score");

        /*topology.setBolt("hdfs", createHdfsBolt(), 4)
                .shuffleGrouping("score");
        topology.setBolt("nodejs", new NodeNotifierBolt(), 4)
                .shuffleGrouping("score");*/

        return topology.createTopology();
    }

    private static Config createConfig(boolean local) {
        int workers = Properties.getInt("sa.storm.workers");
        Config conf = new Config();
        conf.setDebug(true);
        if (local)
            conf.setMaxTaskParallelism(workers);
        else
            conf.setNumWorkers(workers);
        return conf;
    }
}
