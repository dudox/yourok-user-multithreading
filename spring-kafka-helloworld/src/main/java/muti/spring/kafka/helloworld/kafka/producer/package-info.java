/**
 * 
 */
/**
 * @author Andrea Muti
 * created: 21 gen 2018
 * 
 * Create a Spring Kafka Message Producer
 * The KafkaTemplate is used For sending messages. It wraps a Producer and provides methods to send data to Kafka topics. 
 * The template provides asynchronous send methods which return a ListenableFuture.
 * In the KafkaSender class, the KafkaTemplate is auto-wired as the creation will be done further below in a 
 * separate SenderConfig class.
 * 
 * The creation of the KafkaTemplate and KafkaSender is handled in the KafkaSenderConfig class. 
 * The class is annotated with @Configuration which indicates that the class can be used by the 
 * Spring IoC container as a source of bean definitions.
 */
package muti.spring.kafka.helloworld.kafka.producer;