package smartshare.administrationservice.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Configuration
@ConfigurationProperties(prefix = "kafka")
public @Data
class KafkaParameters {
    private String url;
}
