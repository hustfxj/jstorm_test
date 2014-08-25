package jstorm.starter.other;

import org.apache.log4j.Logger;

import java.io.Serializable;


public class ServiceBusSpoutException extends Exception implements Serializable {
    static final Logger logger = Logger.getLogger(ServiceBusSpoutException.class);
    public ServiceBusSpoutException(String message) {
        super(message);
        logger.error(message);
    }
}
