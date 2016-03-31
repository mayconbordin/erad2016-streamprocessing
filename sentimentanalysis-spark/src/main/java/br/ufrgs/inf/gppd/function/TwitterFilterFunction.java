package br.ufrgs.inf.gppd.function;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import twitter4j.Status;

public class TwitterFilterFunction
    implements PairFunction<Status, Long, String>
{
    private static final long serialVersionUID = 42l;
    private final ObjectMapper mapper = new ObjectMapper();

    public Tuple2<Long, String> call(Status status)
    {
        if (status.getLang() != null && status.getLang().equals("en")) {
            return new Tuple2<Long, String>(status.getId(), status.getText());
        }
        return null;
    }
}
