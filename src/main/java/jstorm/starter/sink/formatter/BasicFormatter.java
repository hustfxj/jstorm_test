package jstorm.starter.sink.formatter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class BasicFormatter extends Formatter {

    @Override
    public String format(Tuple tuple) {
        Fields schema = context.getComponentOutputFields(tuple.getSourceComponent(), tuple.getSourceStreamId());
        
        String line = "";
            
        for (int i=0; i<tuple.size(); i++) {
            if (i != 0) line += ", ";
            line += String.format("%s=%s", schema.get(i), tuple.getValue(i));
        }
        
        return line;
    }
    
}
