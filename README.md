[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.sproutsocial/nsq-j/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.sproutsocial/nsq-j)

# nsq-j
Java client for the [NSQ](http://nsq.io) realtime distributed messaging platform

## Install

Add a dependency using Maven
```xml
<dependency>
  <groupId>com.sproutsocial</groupId>
  <artifactId>nsq-j</artifactId>
  <version>1.4.1</version>
</dependency>
```
or Gradle
```
dependencies {
  compile 'com.sproutsocial:nsq-j:1.4.1'
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

### Single NSQ-d host publishing
When we have a single NSQ-d host specified (failoverNsqd is null or not specified when constructing a publisher)
we will reattempt a failed publish after 10 seconds by default.  This happens in line with synchronous publishes or when 
publishing buffered and the batch size is reached.  

If this second attempt fails, the call to publish will throw an NSQException. 

### Failover publishing
nsq-j supports failover publishing.  If you specify a non-null failoverNsqd parameter or manually construct a failover balance strategy with `ListBasedBalanceStrategy#getFailoverStrategyBuilder`

In fail over mode, nsq-j prefers publishing the first element of the provided list of NSQD.  It will fail over to the next nsqd if a publish fails.  After the failover duration, the next publish will attempt to reconnect to the first nsqd.  Failover duration defaults to 5 min.  

If all nsqd are in a failed state (have all failed within the failover duration), the publish will throw an NSQException. 


### Round-robin publishing
To use round robin, construct a balance strategy with  `ListBasedBalanceStrategy#getRoundRobinStrategyBuilder` providing a list of nsqd to use.  

All the hosts that are included in the list will be added to a rotation.  Each publish action is sent
to the next host in the rotation.  If a publishing fails, the host is marked "dead" for the failover duration (5 min default) before
it will be added back to the rotation.  If all hosts are marked dead, an NSQException will be thrown out of publish.  

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

## Development

You must have at least JDK 8 installed. A locally running docker install is also
required to execute the full test suite. The test suite boots a small, local nsq clutser
to exercise the full publish / subscribe flow, including failure modes.

You can run the full test suite, and compile a working jar with:

> make clean all

This will do the necessary setup (pulling docker images), run the test suite, and build the final
jar artifact.

If you just want to execute the test suite, you can use:

> make clean test

