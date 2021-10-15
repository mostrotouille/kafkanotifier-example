package mx.com.mostrotuille.example.kafkanotifier.service;

import java.util.Timer;
import java.util.TimerTask;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import mx.com.mostrotuille.example.kafkanotifier.util.Notifier;
import mx.com.mostrotuille.example.kafkanotifier.util.SftpNotifier;
import mx.com.mostrotuille.example.kafkanotifier.util.SmsNotifier;
import mx.com.mostrotuille.example.kafkanotifier.util.SmtpNotifier;
import mx.com.mostrotuille.example.kafkanotifier.util.WebhookNotifier;

@Service
@Slf4j
public class KafkanotifierService {
	protected class NotifierTask extends TimerTask {
		private Notifier<Long, String> notifier;
		private int retries;
		private Timer timer;

		public NotifierTask(Timer timer, int retries, Notifier<Long, String> notifier) {
			this.timer = timer;
			this.retries = retries;
			this.notifier = notifier;
		}

		public NotifierTask(Timer timer, Notifier<Long, String> notifier) {
			this(timer, 0, notifier);
		}

		@Override
		public void run() {
			if (timer != null) {
				timer.cancel();
				timer = null;
			}

			log.info("{} - Retries[{}]", notifier.getKey(), retries);

			boolean response = false;

			try {
				response = notifier.send();
			} catch (Exception ex) {
				log.error("Notification sending failed.", ex);
			}

			log.info("{} - Response[{}]", notifier.getKey(), response);

			if (!response && retries < MAXIMUM_RETRIES) {
				timer = new Timer();
				timer.schedule(new NotifierTask(timer, retries + 1, notifier),
						retries < RETRIES_DELAY_ARRAY.length ? RETRIES_DELAY_ARRAY[retries] : DEFAULT_RETRIES_DELAY);
			}
		}
	}

	protected static final int DEFAULT_RETRIES_DELAY = 12000;
	protected static final int MAXIMUM_RETRIES = 7;
	protected static final int[] RETRIES_DELAY_ARRAY = new int[] { 3000, 6000, 9000 };

	@KafkaListener(groupId = "${kafka.group.id}", topics = "${kafka.sftp_topic.name}")
	public void consumeSftp(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Long key, String message) {
		final Timer timer = new Timer();
		timer.schedule(new NotifierTask(timer, new SftpNotifier(key, message)), 0);
	}

	@KafkaListener(groupId = "${kafka.group.id}", topics = "${kafka.sms_topic.name}")
	public void consumeSms(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Long key, String message) {
		final Timer timer = new Timer();
		timer.schedule(new NotifierTask(timer, new SmsNotifier(key, message)), 0);
	}

	@KafkaListener(groupId = "${kafka.group.id}", topics = "${kafka.smtp_topic.name}")
	public void consumeSmtp(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Long key, String message) {
		final Timer timer = new Timer();
		timer.schedule(new NotifierTask(timer, new SmtpNotifier(key, message)), 0);
	}

	@KafkaListener(groupId = "${kafka.group.id}", topics = "${kafka.webhook_topic.name}")
	public void consumeWebhook(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Long key, String message) {
		final Timer timer = new Timer();
		timer.schedule(new NotifierTask(timer, new WebhookNotifier(key, message)), 0);
	}
}