Simple Kafka Consumer Aplication is created in Java to demonstrate how connection to the Kafka Broker could be established for a specified period of time(1 minute), during which consumer will subscribe to the topic and consume the data in type of strings. Consumed data is being written in a file and logs are generated. After specified time connection is gracefully shutdown.

Simple Kafka Producer is created just for the practice, in which user is enabled to input of number of messages which will produce and messages are printed to the console. 

Installed Kafka and Zookeper servers are started with below commands:

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

.\bin\windows\kafka-server-start.bat .\config\server.properties

Specified Kafka Consumer application properties  are:

#Consumer receives messages in the pair of key  and value both type of String, therefore we had to define String Deserializer
	
  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
	value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

#If processing messages takes longer time than specified or processing logic is too heavy, consumer will leave the group and start the rebalance. rebalance is the process of changing partition ownership across the consumers

  max.poll.interval.ms=60000

#Heartbeat messages are sent to the consumer coordinator in specified intervals. Heartbeat ensures that consumer session is active, and when new consumer leave 	or join the group rebalance  will start. Heartbeat value must be lower that session.timeout.ms and not more that 1/3.
	
  heartbeat.interval.ms=20000

#The amount of time a consumer can be out of contact with the brokers while still considered alive.
#If heartbeat is not sent before specified time elapsed, conusmer will be marked as failed and will trigger a new round of rebalance
	
  session.timeout.ms=60000

#Specified number of record in one poll
	
  max.poll.records=10000

#Kafka consumer to handle committing offsets automatically

  enable.auto.commit=true

#Kafka consumer will read messages from the beginning
  
  auto.offset.reset=earliest

group.id=consumer-application

bootstrap.servers=localhost:9092

