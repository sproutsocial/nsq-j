[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.sproutsocial/nsq-j/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.sproutsocial/nsq-j)

# nsq-j
Java client for the [NSQ](http://nsq.io) realtime distributed messaging platform

## Install

Add a dependency using Maven
```xml
<dependency>
  <groupId>com.sproutsocial</groupId>
  <artifactId>nsq-j</artifactId>
  <version>1.2</version>
</dependency>
```
or Gradle
```
dependencies {
  compile 'com.sproutsocial:nsq-j:1.2'
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

### Failover publishing
```java
Publisher publisher = new Publisher("nsqd-host", "nsqd-failover-host");
```
If a failover host is provided, it will be used when messages are not able to be written to the primary.  

After a time period of 5 minutes, the next publish will attempt to reconnect to the primary. 
If you wish to change this failback timer, simply use `Publisher#setFailoverDurationSecs`.  

### Round-robin publishing

```java
Publisher publisher = new Publisher("nsqd-host-1:4150,nsqd-host-2:4150,nsqd-host-3:4150");
```
We support round-robin publishing with a simple configuration change.  Providing a comma 
separated list of hosts for either the primary or failover host fields will activate round robin publishing. 

Round robin works by rotation thru all unique host+ports that are configured.  If a publish fails, the connection 
is closed and the host is marked as failed for 5 minutes.  After that time, it will try to reconnect to that host.  
A failed publish will be retried until all hosts are marked as failed.  

You can adjust the failed timer by using `Publisher#setFailoverDurationSecs`.

Round Robin publishing is appropriate for use cases where nsqd is deployed on a different host than the publisher.
This is most comon in autoscaling or ephemeral environemnts.  In such cases its desirable to both have 
well scaled, persistent nsq infrastructure and a single configuration that will make use of that hardware.   

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

