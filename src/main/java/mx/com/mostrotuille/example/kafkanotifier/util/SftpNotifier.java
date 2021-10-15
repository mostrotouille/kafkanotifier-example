package mx.com.mostrotuille.example.kafkanotifier.util;

import java.util.Calendar;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SftpNotifier implements Notifier<Long, String> {
	private Long key;
	private String message;

	public SftpNotifier(Long key, String message) {
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

		final boolean result = (new Random(Calendar.getInstance().getTimeInMillis())).nextBoolean();

		if (!result) {
			throw new Exception("Random error.");
		}

		return result;
	}
}