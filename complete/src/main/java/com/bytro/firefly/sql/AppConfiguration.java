package com.bytro.firefly.sql;

import java.util.Properties;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.bytro.firefly.utils.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by YunusOlgun on 12/14/2016.
 */

@Configuration
public class AppConfiguration {
	@Bean
	public Producer<User, UserGameScoreValue> getProducer() {
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.101.10:9092");
		producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
		producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
		producerConfig.put("schema.registry.url", "http://192.168.101.10:8081");
		return new KafkaProducer<>(producerConfig);
	}
}
