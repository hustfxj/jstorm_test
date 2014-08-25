package jstorm.starter.spoult;

import jstorm.starter.other.IServiceBusQueueDetail;
import jstorm.starter.other.ServiceBusSpoutException;
import backtype.storm.spout.*;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import jstorm.starter.other.ServiceBusQueueConnection;
import jstorm.starter.other.ServiceBusTopicConnection;

import org.apache.log4j.Logger;

import java.util.Map;

public class ServiceBusQueueSpout extends BaseRichSpout {
    private IServiceBusQueueDetail detail;
    private SpoutOutputCollector collector;
    private long processedMessages = 0L;

    static final Logger logger = Logger.getLogger(ServiceBusQueueSpout.class);

    public ServiceBusQueueSpout(IServiceBusQueueDetail detail)  {
         this.detail = detail;
    }

    public ServiceBusQueueSpout(String connectionString, String queueName) throws ServiceBusSpoutException  {
        this.detail = new ServiceBusQueueConnection(connectionString, queueName);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        try {
            logger.info("connecting to service bus queue " + this.detail.getQueueName());
            this.detail.connect();
            this.collector = spoutOutputCollector;
        }
        catch(ServiceBusSpoutException sbpe) { 
        	/* log this somewhere - maybe another service bus exception queue */
        }

    }

    @Override
    public void close() {
        logger.info("closing service bus contract ");
    }

    @Override
    public void nextTuple() {
        // we'll try this on the main thread - if there is a problem then we'll implement runnable
        // check performance against this approach but we can let the spout scale rather than scale ourselves
        try {
            logger.info("attempting to get next message from queue " + this.detail.getQueueName());
            if(!this.detail.isConnected())
                return;
            
            Utils.sleep(5000);
            try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            
            // this message can be anything - most likely JSON but we don't impose a structure in the spout
            String message = this.detail.getNextMessageForSpout();
            logger.info("Received message is null: " + (message == null));

            //pointless emitting a null message
            if(message == null)
                return;

            collector.emit(new Values(message));
            processedMessages++;
        }
        catch(ServiceBusSpoutException sbse)    {
            // if this occurs we probably want to passthru - maybe a short sleep to unlock the thread
            // TODO: look at adding a retry-fail strategy if this continually dies then it maybe that we're connected but something
            logger.error(sbse.getMessage());
            // has happened to the SB namespace
            try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    public Boolean isConnected()    {
        return this.detail.isConnected();
    }

    public long getProcessedMessageCount()  {
        return this.processedMessages;
    }
}
