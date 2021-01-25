# nsqauthj

## What is nsqauthj?

`nsqauthj` is a Java implementation of an NSQ auth server.

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

## NSQ Auth Failover Configuration
NSQ Auth validates tokens by querying Vault and verifying that the token is valid. 
In that case that Vault is down, tokens would not be validated and therefore only `Publish Only` tokens could be used. 

Due to planned downtime or an unexpected outage in Vault, NSQ Auth can be set to `fail open` in order to continue service functionality during downtime. 
To fail open, set the `failOpen` value in the `tokenValidation` configuration block to `true`. 
```
tokenValidation:
  serviceTokenPath: secret/services/nsq/service-tokens/
  userTokenPath: secret/services/nsq/user-tokens/
  tokenTTL: 3600
  failOpen: true
```

This will cause all tokens to have `subscribe` and `publish` capabilities. NOTE: Because of security reasons, this should only be set to true when necessary. 
`failOpen` is set to false by default. 
 
# Running Tests

Tests are run with maven.  There are both unit tests and integration tests!

If you are using Intellij, you may want to use the Maven test lifecycle to run tests as opposed to relying on built-in
junit integration or tests may fail (because they are not unittests).

```
# Running the Unit Tests
$ mvn test

# Running the Integration Tests
$ mvn verify
```

## Developing Locally

This project ships with a working development environment using docker-compose.
```
$ docker-compose up
```

To run the application locally, you can use the `example-config.yml` that ships with this repo as it is configured to work locally.

```
# Make the jar
$ mvn clean package

# Run the jar
$ java -Dcommons.config=yaml:example-config.yml -jar target/nsqauthj-0.0.1.jar server
```

NOTE: integration tests **do not** use docker-compose.  If making updates to `docker-compose.yml`, you'll also want to update
the configuration of the `docker-maven-plugin` in the pom.


## Deployment

TODO: This should use Oak!
