package com.sproutsocial.nsq;

import java.util.List;

class LookupResponse {

    private LookupResponse data; //older versions wrap reponses in a "data" field
    private List<Producer> producers;

    public LookupResponse getData() {
        return data;
    }

    public void setData(LookupResponse data) {
        this.data = data;
    }

    public List<Producer> getProducers() {
        return producers;
    }

    public void setProducers(List<Producer> producers) {
        this.producers = producers;
    }

    @Override
    public String toString() {
        return "LookupResponse{" +
                "data=" + data +
                ", producers=" + producers +
                '}';
    }

    static class Producer {
        private String broadcastAddress;
        private int tcpPort;

        public String getBroadcastAddress() {
            return broadcastAddress;
        }

        public void setBroadcastAddress(String broadcastAddress) {
            this.broadcastAddress = broadcastAddress;
        }

        public int getTcpPort() {
            return tcpPort;
        }

        public void setTcpPort(int tcpPort) {
            this.tcpPort = tcpPort;
        }

        @Override
        public String toString() {
            return "Producer{" +
                    "broadcastAddress='" + broadcastAddress + '\'' +
                    ", tcpPort=" + tcpPort +
                    '}';
        }
    }
}
