FROM 412335208158.dkr.ecr.us-east-1.amazonaws.com/sprout-java:openjdk-22.04-linux-amd64-latest

RUN ["useradd", "--home-dir", "/srv/nsqauthj", "--create-home", "nsqauthj"]

USER nsqauthj
WORKDIR /srv/nsqauthj

COPY ./target/nsqauthj-*.jar ./
RUN mv nsqauthj-*.jar nsqauthj.jar
