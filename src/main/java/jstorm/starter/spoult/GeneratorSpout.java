package jstorm.starter.spoult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jstorm.starter.constants.BaseConstants.BaseConf;
import jstorm.starter.spoult.generator.Generator;
import jstorm.starter.tool.AbstractSpout;
import jstorm.starter.util.ClassLoaderUtils;
import jstorm.starter.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
    private Generator generator;

    @Override
    protected void initialize() {
        String generatorClass = config.getString(getConfigKey(BaseConf.SPOUT_GENERATOR));
        generator = (Generator) ClassLoaderUtils.newInstance(generatorClass, "parser", LOG);
        generator.initialize(config);
    }

    @Override
    public void nextTuple() {
        StreamValues values = generator.generate();
        collector.emit(values.getStreamId(), values);
    }
}
