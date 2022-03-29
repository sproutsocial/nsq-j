FROM 412335208158.dkr.ecr.us-east-1.amazonaws.com/sprout-java

RUN ["useradd", "--home-dir", "/srv/nsqauthj", "--create-home", "nsqauthj"]

USER nsqauthj
WORKDIR /srv/nsqauthj

COPY ./target/nsqauthj-*.jar ./
RUN mv nsqauthj-*.jar nsqauthj.jar
