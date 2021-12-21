package muti.kafka.twitter.app;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaTwitterProducer {
	
	public static void main(String[] args) throws Exception {
		
		args = new String[10];
		args[0] = "LB724DpRhIYjSdDiFS6t79Ydv"; 							// consumerKey
		args[1] = "K9LwrBn3mFfA4Fh9AB7iObPXLkWje1SBiyXUSFxi4hVHdT0p0Y"; // consumerSecret
		args[2] = "313345616-v15cuAMaVSCdNocP4VZpLz0FNDTsKXqmYf1eoW4S"; // accessToken
		args[3] = "RhAk3Tda1dQHMOcC9YmToP6CRI48ykjpXTrjHRbsi869v"; 		// accessTokenSecret
		args[4] = "my-first-topic"; // topicName
		args[5] = "food"; // key word
//		args[6] = "";
		
		LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

//		if (args.length < 5) {
//			System.out.println(
//					"Usage: KafkaTwitterProducer <twitter-consumer-key>"+
//							"<twitter-consumer-secret> <twitter-access-token>"+
//							"<twitter-access-token-secret>"+
//					"<topic-name> <twitter-search-keywords>");
//			return;
//		}

		String consumerKey = args[0].toString();
		String consumerSecret = args[1].toString();
		String accessToken = args[2].toString();
		String accessTokenSecret = args[3].toString();
		String topicName = args[4].toString();
		
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {      
				queue.offer(status);

				// System.out.println("@" + status.getUser().getScreenName()  + " - " + status.getText());
				// System.out.println("@" + status.getUser().getScreen-Name());

				/*for(URLEntity urle : status.getURLEntities()) {
               System.out.println(urle.getDisplayURL());
            }*/

				/*for(HashtagEntity hashtage : status.getHashtagEntities()) {
               System.out.println(hashtage.getText());
            }*/
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				// System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// System.out.println("Got track limitation notice:" +  numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				// System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
			}      

			@Override
			public void onStallWarning(StallWarning warning) {
				// System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);

		FilterQuery query = new FilterQuery().track(keyWords);
		twitterStream.filter(query);

		Thread.sleep(5000);

		//Add Kafka producer config settings
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		int i = 0;
		int j = 0;

		while (i < 10) {
			Status ret = queue.poll();

			if (ret == null) {
				Thread.sleep(100);
				i++;
			} else {
				for (HashtagEntity hashtage : ret.getHashtagEntities()) {
					System.out.println("Hashtag: " + hashtage.getText());
					producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), hashtage.getText()));
				}
			}
		}
		producer.close();
		Thread.sleep(5000);
		twitterStream.shutdown();
	}
}