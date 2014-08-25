package jstorm.starter.other;



// Include the following imports to use service bus APIs
import com.microsoft.windowsazure.services.serviceBus.*;
import com.microsoft.windowsazure.services.serviceBus.models.*;
import com.microsoft.windowsazure.services.core.*;


import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.io.StringWriter;


public class ServiceBusTopicConnection implements IServiceBusTopicDetail, Serializable {

    private String connectionString;
    private String topicName;
    private Integer messageCount;
    private ServiceBusContract serviceBusContract = null;
    private Boolean isConnected = false;
    private String filter;
    private String subscriptionName;

    static final Logger logger = Logger.getLogger(ServiceBusTopicConnection.class);

    public ServiceBusTopicConnection(String connectionString, String topicName, String subscriptionName, String filter) throws ServiceBusSpoutException {
        this.connectionString = connectionString;
        this.topicName = topicName;
        this.subscriptionName = subscriptionName != null ? subscriptionName : topicName + "sub";
        this.filter = filter;
    }

    @Override
    public String getConnectionString() throws ServiceBusSpoutException {
        if(this.connectionString.split(";").length != 3)    {
            throw new ServiceBusSpoutException("incorrectly formatted connection string");
        }

        return this.connectionString;
    }

    @Override
    public String getTopicName() throws ServiceBusSpoutException  {
        if(this.topicName.length() > 18 || this.topicName.length() < 3) {
            throw new ServiceBusSpoutException("incorrect length of topic name must be > 3 and < 11");
        }
        return this.topicName.toLowerCase();
    }

    @Override
    public Integer getTotalMessageCount()   {
        return this.messageCount;
    }

    @Override
    public String getSubscriptionName() throws ServiceBusSpoutException{
        return subscriptionName;
    }

    @Override
    public String getFilterQuery() {
        return this.filter;
    }

    @Override
    public void connect() throws ServiceBusSpoutException {
        Configuration configuration = new Configuration();
        configuration = ServiceBusConfiguration.configureWithConnectionString(null, configuration, getConnectionString());

        serviceBusContract = ServiceBusService.create(configuration);
        TopicInfo topicInfo = new TopicInfo(getTopicName());
        try
        {
            // create the new topic
            try {
                CreateTopicResult ctResult = serviceBusContract.createTopic(topicInfo);
            }
            catch(ServiceException se)  {
                //if(se.getHttpStatusCode() != 409)
                    //throw se;
            }

            String subscriptionName = getSubscriptionName();
            // create a subscription for the topic
            SubscriptionInfo subInfo = new SubscriptionInfo(subscriptionName);


            try {
                CreateSubscriptionResult result = serviceBusContract.createSubscription(getTopicName(), subInfo);
            }
            catch(ServiceException se)  {
                //if(se.getHttpStatusCode() != 409)
                    //throw se;
            }

            if(this.filter != null && !this.filter.isEmpty())  {
                RuleInfo ruleInfo = new RuleInfo();
                ruleInfo = ruleInfo.withSqlExpressionFilter(filter);
                CreateRuleResult ruleResult = serviceBusContract.createRule(getTopicName(), subscriptionName, ruleInfo);
            }
            // chances are if this fails we'll be looking at the queue already existing
            isConnected = true;
        }
        catch (ServiceException e)
        {
            if(e.getHttpStatusCode() == 409)    {
                isConnected = true;
            }
            else    {
                throw new ServiceBusSpoutException(e.getMessage());
            }
        }
    }

    @Override
    public String getNextMessageForSpout() throws ServiceBusSpoutException {

        if(!isConnected())  {
            throw new ServiceBusSpoutException("call connect() before trying to receive messages");
        }
        ReceiveMessageOptions receiveOptions = ReceiveMessageOptions.DEFAULT;
        receiveOptions.setReceiveMode(ReceiveMode.PEEK_LOCK);

        BrokeredMessage message = null;
        try {

            ReceiveSubscriptionMessageResult receive = serviceBusContract.receiveSubscriptionMessage(getTopicName(), getSubscriptionName(), receiveOptions);
            message = receive.getValue();

            if (message != null && message.getMessageId() != null)
            {
                // get the string value from the body
                StringWriter writer = new StringWriter();
                IOUtils.copy(message.getBody(), writer);
                String messageBody = writer.toString();
                // spit this out ...
                System.out.println("message arrived with value " + messageBody);
                serviceBusContract.deleteMessage(message);
                return messageBody;
            }
        }
        catch(Exception se)  {
            try{
                serviceBusContract.unlockMessage(message);
            }
            // not sure whether we want to stop if we can't deal with this
            catch(Exception ex) {
                throw new ServiceBusSpoutException(se.getMessage());
            }
        }
        return null;
    }

    @Override
    public Boolean isConnected() {
        return this.isConnected;
    }
}
