package smartshare.administrationservice.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import smartshare.administrationservice.dto.BucketObjectEvent;
import smartshare.administrationservice.dto.SagaEvent;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfiguration {

    private final Map<String, Object> consumerConfigurationProperties = new HashMap<>();
    private final Map<String, Object> producerConfigurationProperties = new HashMap<>();

    KafkaConfiguration() {

        producerConfigurationProperties.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092" );
        producerConfigurationProperties.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
        producerConfigurationProperties.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class );


        consumerConfigurationProperties.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092" );
        consumerConfigurationProperties.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true );
        consumerConfigurationProperties.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
        consumerConfigurationProperties.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class );

    }

    @Bean
    public ProducerFactory<String, SagaEvent> producerFactory() {
        return new DefaultKafkaProducerFactory<>( producerConfigurationProperties );
    }

    @Bean
    public KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate() {

        return new KafkaTemplate<>( producerFactory() );
    }

    @Bean
    public ConsumerFactory<String, BucketObjectEvent[]> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>( consumerConfigurationProperties, new StringDeserializer(), new JsonDeserializer<>( BucketObjectEvent[].class, false ) );
    }

    @Bean
    public ConsumerFactory<String, String> bucketAdministrationConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>( consumerConfigurationProperties, new StringDeserializer(), new StringDeserializer() );
    }

    @Bean
    public ConsumerFactory<String, SagaEvent> sagaEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>( consumerConfigurationProperties, new StringDeserializer(), new JsonDeserializer<>( SagaEvent.class, false ) );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, BucketObjectEvent[]> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, BucketObjectEvent[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory( consumerFactory() );
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SagaEvent> SagaEventKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SagaEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory( sagaEventConsumerFactory() );
        factory.setReplyTemplate( sagaEventKafkaTemplate() );
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> bucketAdministrationConsumerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory( bucketAdministrationConsumerFactory() );
        return factory;
    }

    @Bean
    public NewTopic sagaAccessTopic() {
        return TopicBuilder.name( "sagaAccess" ).compact().build();
    }

    @Bean
    public NewTopic BucketAccessManagementTopic() {
        return TopicBuilder.name( "BucketAccessManagement" ).compact().build();
    }

    @Bean
    public NewTopic BucketObjectAccessManagementTopic() {
        return TopicBuilder.name( "BucketObjectAccessManagement" ).compact().build();
    }

}
