package com.anomalous.detection;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;

@SpringBootApplication
@EnableBinding(AnomalousDetectionStreamApplication.AnomalousStreamProcess.class)
public class AnomalousDetectionStreamApplication {

	public static void main(String[] args) {
		SpringApplication.run(AnomalousDetectionStreamApplication.class, args);
	}
	
	@StreamListener(AnomalousStreamProcess.INPUT)
	public void streamProcess(KStream<String, String> stream) {
		
		KTable<Windowed<String>, Long> table = stream
			.filter( (ignoredKey,value) -> value != null)
			.mapValues((ignoredKey,value) -> value.toUpperCase())
			.map( (ignoredKey,value) -> new KeyValue<>(value, value))
			.groupByKey()
			.windowedBy(TimeWindows.of(Duration.ofMinutes(1).toMillis()))
			.count();
		
		table
			
			.toStream()
			.filter( (windowedKey,v) -> v != null)
			.filter( (windowedKey,v) -> v > 3)
			.peek( (k,v)-> System.out.printf("ALERT the user %s has %d times to access\n",k.key(),v) );
	}
	
	
	interface AnomalousStreamProcess{
		public static final String  INPUT = "input";
		@Input(INPUT)
		public KStream<String,String> streamProcess();
	}
	
}
