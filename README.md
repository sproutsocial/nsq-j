[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.sproutsocial/nsq-j/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.sproutsocial/nsq-j)

# nsq-j
Java client for the [NSQ](http://nsq.io) realtime distributed messaging platform

## Install

Add a dependency using Maven
```xml
<dependency>
  <groupId>com.sproutsocial</groupId>
  <artifactId>nsq-j</artifactId>
  <version>0.9.4</version>
</dependency>
```
or Gradle
```
dependencies {
  compile 'com.sproutsocial:nsq-j:0.9.4'
}
```

## Publish
```java
Publisher publisher = new Publisher("nsqd-host");

byte[] data = "Hello nsq".getBytes();
publisher.publishBuffered("example_topic", data);
```
Buffers messages to improve performance (to 16k or 300 milliseconds by default),

`publisher.publish("example_topic", data)` publishes synchronously and returns
after nsqd responds `OK`

You can batch messages manually and publish them all at once with
`publish(String topic, List<byte[]> messages)`

## Subscribe
```java
public class PubExample {

    public static void handleData(byte[] data) {
        System.out.println("Received:" + new String(data));
    }

    public static void main(String[] args) {
        Subscriber subscriber = new Subscriber("lookup-host-1", "lookup-host-2");
        subscriber.subscribe("example_topic", "test_channel", PubExample::handleData);
    }
}
```
Uses the lookup-hosts to discover any servers publishing example_topic.

If handleData returns normally `FIN` is sent to nsqd to finish the message,
if the handler throws any exception then the message is requeued and processing is
delayed using exponential backoff. Delay times and max attempts  are configurable.

For complete control handle the Message directly:
```java
    public static void handleMessage(Message msg) {
        try {
            byte[] data = msg.getData();
            // ... complicated work ...
            msg.finish();
        }
        catch (Exception e) {
            msg.requeue();
        }
    }
```

Publishers and Subscribers are thread safe and should be reused.
Your handler methods should be thread safe, make them `synchronized` if you are unsure.

`Client.getDefaultClient().stop()` waits for in-flight messages, closes all connections
and allows all threads to exit.

[Javadocs](https://sproutsocial.github.io/nsq-j/)

