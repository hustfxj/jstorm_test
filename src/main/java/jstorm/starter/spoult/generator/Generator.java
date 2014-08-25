package jstorm.starter.spoult.generator;

import java.util.Map;
import jstorm.starter.util.Configuration;
import jstorm.starter.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Generator {
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }
    
    public abstract StreamValues generate();
}
