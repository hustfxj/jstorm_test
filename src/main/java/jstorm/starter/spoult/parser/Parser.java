package jstorm.starter.spoult.parser;

import java.util.Arrays;
import java.util.List;
import jstorm.starter.util.Configuration;
import jstorm.starter.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Parser {
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }

    public abstract List<StreamValues> parse(String input);
    
    protected List<StreamValues> list(StreamValues...values) {
        return Arrays.asList(values);
    }
}