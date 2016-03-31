package br.ufrgs.inf.gppd.nlp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;

public class PositiveWords implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PositiveWords.class);
    public static final long serialVersionUID = 42L;
    private Set<String> posWords;
    private static PositiveWords _singleton;

    private PositiveWords() {
        posWords = new HashSet<String>();
        BufferedReader rd = null;

        try {
            rd = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/pos-words.txt")));

            String line;
            while ((line = rd.readLine()) != null) {
                posWords.add(line);
            }
        } catch (IOException ex) {
            LOGGER.error("IO error while initializing", ex);
        } finally {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }
    }

    private static PositiveWords get() {
        if (_singleton == null)
            _singleton = new PositiveWords();
        return _singleton;
    }

    public static Set<String> getWords() {
        return get().posWords;
    }
}
