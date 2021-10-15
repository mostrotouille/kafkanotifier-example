package mx.com.mostrotuille.example.kafkanotifier.rest;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import mx.com.mostrotuille.example.kafkanotifier.dto.Message;
import mx.com.mostrotuille.example.kafkanotifier.dto.Response;

@RestController
@RequestMapping("/kafka")
@Slf4j
public class KafkanotifierController {
	protected static final int TIMEOUT = 3000;

	@Autowired
	private KafkaTemplate<Long, String> kafkaTemplate;

	@Value(value = "${kafka.sftp_topic.name}")
	private String sftpTopicName;

	@Value(value = "${kafka.sms_topic.name}")
	private String smsTopicName;

	@Value(value = "${kafka.smtp_topic.name}")
	private String smtpTopicName;

	@Value(value = "${kafka.webhook_topic.name}")
	private String webhookTopicName;

	public Long createUniqueKey() {
		return Calendar.getInstance().getTimeInMillis();
	}

	@PostMapping(value = "/notify", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> notify(@RequestBody Message message) {
		String topicName = null;

		try {
			switch (message.getType().toUpperCase()) {
			case "SFTP":
				topicName = sftpTopicName;
				break;
			case "SMS":
				topicName = smsTopicName;
				break;
			case "SMTP":
				topicName = smtpTopicName;
				break;
			case "WEBHOOK":
				topicName = webhookTopicName;
				break;
			default:
				break;
			}

			final Long key = createUniqueKey();

			kafkaTemplate.send(new ProducerRecord<>(topicName, key, message.getContent())).get(TIMEOUT,
					TimeUnit.MILLISECONDS);

			final Response response = Response.builder().status(HttpStatus.OK).timestamp(LocalDateTime.now()).key(key)
					.build();

			return new ResponseEntity<>(response, response.getStatus());
		} catch (Exception ex) {
			final String errorMessage = "Produce message failed.";

			log.error(errorMessage, ex);

			final Response response = Response.builder().status(HttpStatus.BAD_REQUEST).timestamp(LocalDateTime.now())
					.message(errorMessage).errors(Arrays.asList(ex.getMessage())).build();

			return new ResponseEntity<>(response, response.getStatus());
		}
	}
}