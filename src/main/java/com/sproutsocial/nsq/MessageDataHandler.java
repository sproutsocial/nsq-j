package com.sproutsocial.nsq;

public interface MessageDataHandler {

    void accept(byte[] data);

}
