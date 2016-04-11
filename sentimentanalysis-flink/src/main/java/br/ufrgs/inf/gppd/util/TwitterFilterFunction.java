package br.ufrgs.inf.gppd.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

    public class TwitterFilterFunction implements FlatMapFunction<String, Tuple2<Long, String>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        /**
         * Select the language from the incoming JSON text
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<Long, String>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new org.codehaus.jackson.map.ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").getTextValue().equals("en");
            boolean hasText = jsonNode.has("text");
            boolean hasId = jsonNode.has("id");

            if (isEnglish && hasText && hasId) {
                // message of tweet
                String result = jsonNode.get("text").getTextValue();
                Long id = jsonNode.get("id").getLongValue();
                // split the message
//                while (tokenizer.hasMoreTokens()) {
//                    String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

                    if (!result.equals("")) {
                        out.collect(new Tuple2<>(id, result));
                    }
                }

        }
    }

