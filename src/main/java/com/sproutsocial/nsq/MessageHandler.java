package com.sproutsocial.nsq;

public interface MessageHandler {

    void accept(Message msg);

}
