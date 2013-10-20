package ch.trivadis.sample.twitter;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class StreamingCollectorRunner {

	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"classpath:applicationContext.xml");



		StreamCollector obj12 = (StreamCollector) context
				.getBean("filterStreamCollector-12");
		obj12.start();
	}

}
