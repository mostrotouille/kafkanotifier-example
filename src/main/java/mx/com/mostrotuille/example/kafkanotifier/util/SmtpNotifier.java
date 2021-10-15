package mx.com.mostrotuille.example.kafkanotifier.util;

import java.util.Calendar;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmtpNotifier implements Notifier<Long, String> {
	private Long key;
	private String message;

	public SmtpNotifier(Long key, String message) {
		this.key = key;
		this.message = message;
	}

	@Override
	public Long getKey() {
		return key;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public boolean send() throws Exception {
		log.info("{} - Message[{}]", key, message);

		return (new Random(Calendar.getInstance().getTimeInMillis())).nextBoolean();
	}
}