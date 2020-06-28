package smartshare.administrationservice.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
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

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfiguration {

    private final Map<String, Object> consumerConfigurationProperties = new HashMap<>();
    private final Map<String, Object> producerConfigurationProperties = new HashMap<>();
    private final KafkaParameters kafkaParameters;

    @Autowired
    KafkaConfiguration(KafkaParameters kafkaParameters) {
        this.kafkaParameters = kafkaParameters;
        log.info( "Assigned kafka Url in KafkaConsumerConfiguration : " + this.kafkaParameters.getUrl() );
        producerConfigurationProperties.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaParameters.getUrl() );
        producerConfigurationProperties.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
        producerConfigurationProperties.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class );


        consumerConfigurationProperties.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaParameters.getUrl() );
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
    public NewTopic sagaAccessResultTopic() {
        return TopicBuilder.name( "sagaAccessResult" ).compact().build();
    }

    @Bean
    public NewTopic bucketAccessManagementTopic() {
        return TopicBuilder.name( "BucketAccessManagement" ).compact().build();
    }

    @Bean
    public NewTopic bucketObjectAccessManagementTopic() {
        return TopicBuilder.name( "BucketObjectAccessManagement" ).compact().build();
    }

}
