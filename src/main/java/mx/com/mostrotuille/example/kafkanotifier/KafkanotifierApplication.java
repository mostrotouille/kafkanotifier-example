package mx.com.mostrotuille.example.kafkanotifier;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@EnableKafka
@SpringBootApplication
public class KafkanotifierApplication {
	public static void main(String[] args) {
		SpringApplication.run(KafkanotifierApplication.class, args);
	}

	@Value(value = "${kafka.bootstrap_servers}")
	private String bootstrapServers;

	@Value(value = "${kafka.group.id}")
	private String groupId;

	@Value(value = "${kafka.sftp_topic.name}")
	private String sftpTopicName;

	@Value(value = "${kafka.sms_topic.name}")
	private String smsTopicName;

	@Value(value = "${kafka.smtp_topic.name}")
	private String smtpTopicName;

	@Value(value = "${kafka.webhook_topic.name}")
	private String webhookTopicName;

	@Bean
	public ConsumerFactory<Long, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerProperties());
	}

	@Bean
	public Map<String, Object> consumerProperties() {
		final Map<String, Object> result = new HashMap<>();
		result.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		result.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		result.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		result.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return result;
	}

	@Bean
	public KafkaAdmin kafkaAdmin() {
		final Map<String, Object> config = new HashMap<>();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		return new KafkaAdmin(config);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<Long, String> kafkaListenerContainerFactory() {
		final ConcurrentKafkaListenerContainerFactory<Long, String> result = new ConcurrentKafkaListenerContainerFactory<>();
		result.setConsumerFactory(consumerFactory());

		return result;
	}

	@Bean
	public KafkaTemplate<Long, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ProducerFactory<Long, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerProperties());
	}

	@Bean
	public Map<String, Object> producerProperties() {
		final Map<String, Object> result = new HashMap<>();
		result.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		result.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		result.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		result.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
		result.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		result.put(ProducerConfig.RETRIES_CONFIG, 3);
		result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return result;
	}

	@Bean
	public NewTopic sftpTopic() {
		return new NewTopic(sftpTopicName, 5, (short) 1);
	}

	@Bean
	public NewTopic smsTopic() {
		return new NewTopic(smsTopicName, 5, (short) 1);
	}

	@Bean
	public NewTopic smtpTopic() {
		return new NewTopic(smtpTopicName, 5, (short) 1);
	}

	@Bean
	public NewTopic webhookTopic() {
		return new NewTopic(webhookTopicName, 5, (short) 1);
	}
}