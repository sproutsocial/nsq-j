
### NSQ docs
[protocol](http://nsq.io/clients/tcp_protocol_spec.html)

[client requirements](http://nsq.io/clients/building_client_libraries.html)

### Features
- Resilience to network weirdness - watchdog timer closes connections if heartbeats from nsqd go missing.
- Resilience to handler/client weirdness - watchdog timer ignores in-flight msgs after nsqd msg timeout.
- Synchronous PUB - publish function does not return until nsqd acks - use MPUB for better performance.
- Asynchronous FIN - configurable delay before flushing socket with pending FINs.
- Graceful shutdown - tell nsqd to stop sending msgs (RDY 0) and wait for in-flight msgs to be handled.

### Design
One thread per connection using synchronous io.
(Remember 1MB/thread stack is the default on 64-bit linux, consider -Xss512k)

One package with mostly package-protected visibility.

### For now implement everything except:
- Backoff 
- TLS
- Authentication
- Deflate compression (at least on java 6, java 7 supports it without a library)

### Plan to open source it
- Target java 6
- Dependencies: gauva, jackson, snappy-java (jni)
- Unit and integration tests, especially integration tests that use a real nsqd

