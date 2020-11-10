# nsqauthj

## What is nsqauthj?

`nsqauthj` is a Java implementation of an NSQ auth server.

This uses Vault as a backend for its reference backend.

## How does it work?

`nsqauthj` exposes a single endpoint `/auth` that accepts a single parameter `secret`.  `nsqauthj` then determines
if the token is valid by checking Vault for the prescence and permissions of that token.  If a provided token does not exist,
a publish only token is generated.

Valid tokens stored in Vault must contain both a `username` and valid list of `topics`.

Example
```
$ vault kv get secret/services/nsq/service-tokens/abcd
====== Metadata ======
Key              Value
---              -----
created_time     2020-11-10T00:06:54.9258873Z
deletion_time    n/a
destroyed        false
version          1

====== Data ======
Key         Value
---         -----
topics      .*
username    example_service_token
```

### Types of Token

There are three different types of tokens.
1. User Tokens
2. Service Tokens
3. Publish Only Token

*Service Tokens* are for running applications.  These are created manually.  These have both publish and subscribe
permissions on all topics and channels.

*User Tokens* are created by Orchard and are for humans to use.  These tokens have publish and subscribe but
can only subscribe to channels ending in `ephemeral`.  This is so that any channel created by a human will disappear when
the consumer is finished.

*Publish Only Tokens* only grant the ability to publish.  These exists so that if Vault is down, we are still able to
enqueue messages in NSQ even if we cannot consume them.

## Running Tests

Tests are run with maven.  There are both unit tests and integration tests!

```
# Running the Unit Tests
$ mvn test

# Running the Integration Tests
$ mvn verify
```

## Deployment

TODO: This should use Oak!
