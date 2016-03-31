package br.ufrgs.inf.gppd.bolt;

import au.com.bytecode.opencsv.CSVWriter;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class CsvBolt extends BaseBasicBolt {
    private transient static final Logger logger = Logger.getLogger(CsvBolt.class);

    private FileWriter fileWriter;
    private CSVWriter csvWriter;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);

        try {
            fileWriter = new FileWriter("/tmp/sentiment.csv");
            csvWriter = new CSVWriter(fileWriter, ',');
        } catch (IOException ex) {
            logger.error("Unable to create CSV file", ex);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();

        try {
            fileWriter.close();
            csvWriter.close();
        } catch (IOException ex) {
            logger.error("Unable to close CSV file", ex);
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<String> row = new ArrayList<>();

        for (Object value : tuple.getValues()) {
            row.add(value.toString());
        }

        String[] stringArray = row.toArray(new String[0]);
        csvWriter.writeNext(stringArray);
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}