package twitter_kafka;

public class KafkaConfiguration {
    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String TOPIC = "covid-tweets";
    public static final long SLEEP_TIMER = 1000;
    
    public static String CLIENT_ID="client1";    
    public static String GROUP_ID_CONFIG="consumerGroup1";
    
    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";
    
    public static Integer MAX_POLL_RECORDS=1;
    public static Integer MESSAGE_SIZE=100000000;
    public static Integer MESSAGE_COUNT=1000;
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
}
