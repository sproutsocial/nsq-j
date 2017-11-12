# nsq-j
Java client for the NSQ realtime distributed messaging platform

### Running Integration Tests

You'll need a valid instance of nsqd running locally to be able to run the integration tests. One way to easily 
bring up the required nsqd processes is to use docker-compose. You'll need:

- [Docker](https://docs.docker.com/docker-for-mac/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Then you can simply run:

    docker-compose up -d
    
This will bring up the required docker containers and map the necessary ports locally. To clean up, just run:

    docker-compose down
    