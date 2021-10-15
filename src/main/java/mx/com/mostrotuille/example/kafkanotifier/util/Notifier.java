package mx.com.mostrotuille.example.kafkanotifier.util;

public interface Notifier<K, M> {
	public K getKey();

	public M getMessage();

	public boolean send() throws Exception;
}