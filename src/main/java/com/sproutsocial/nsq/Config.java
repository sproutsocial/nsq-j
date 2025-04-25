package com.sproutsocial.nsq;

/**
 * Configuration sent to nsqd with the IDENTIFY command
 * http://nsq.io/clients/tcp_protocol_spec.html#identify
 * to negotiate the features to use on a connection.
 */
public class Config {

    private String clientId;
    private String hostname;
    private Boolean featureNegotiation = true;
    private Integer heartbeatInterval;
    private Integer outputBufferSize;
    private Integer outputBufferTimeout;
    private Boolean tlsV1;
    private Boolean snappy;
    private Boolean deflate;
    private Integer deflateLevel;
    private Integer sampleRate;
    private String userAgent = "nsq-j/2.0.0";
    private Integer msgTimeout;

    private boolean warnWhenNotUsingTls = true;

    //region accessors
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Boolean getFeatureNegotiation() {
        return featureNegotiation;
    }

    public void setFeatureNegotiation(Boolean featureNegotiation) {
        this.featureNegotiation = featureNegotiation;
    }

    public Integer getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Integer heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Integer getOutputBufferSize() {
        return outputBufferSize;
    }

    public void setOutputBufferSize(Integer outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }

    public Integer getOutputBufferTimeout() {
        return outputBufferTimeout;
    }

    public void setOutputBufferTimeout(Integer outputBufferTimeout) {
        this.outputBufferTimeout = outputBufferTimeout;
    }

    public Boolean getTlsV1() {
        return tlsV1;
    }

    public void setTlsV1(Boolean tlsV1) {
        this.tlsV1 = tlsV1;
    }

    public Boolean getSnappy() {
        return snappy;
    }

    public void setSnappy(Boolean snappy) {
        this.snappy = snappy;
    }

    public Boolean getDeflate() {
        return deflate;
    }

    public void setDeflate(Boolean deflate) {
        this.deflate = deflate;
    }

    public Integer getDeflateLevel() {
        return deflateLevel;
    }

    public void setDeflateLevel(Integer deflateLevel) {
        this.deflateLevel = deflateLevel;
    }

    public Integer getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public Integer getMsgTimeout() {
        return msgTimeout;
    }

    public void setMsgTimeout(Integer msgTimeout) {
        this.msgTimeout = msgTimeout;
    }

    public boolean isWarnWhenNotUsingTls() {
        return warnWhenNotUsingTls;
    }

    public void setWarnWhenNotUsingTls(boolean warnWhenNotUsingTls) {
        this.warnWhenNotUsingTls = warnWhenNotUsingTls;
    }

    //endregion

    @Override
    public String toString() {
        return "Config{" +
                "clientId='" + clientId + '\'' +
                ", hostname='" + hostname + '\'' +
                ", featureNegotiation=" + featureNegotiation +
                ", heartbeatInterval=" + heartbeatInterval +
                ", outputBufferSize=" + outputBufferSize +
                ", outputBufferTimeout=" + outputBufferTimeout +
                ", tlsV1=" + tlsV1 +
                ", snappy=" + snappy +
                ", deflate=" + deflate +
                ", deflateLevel=" + deflateLevel +
                ", sampleRate=" + sampleRate +
                ", userAgent='" + userAgent + '\'' +
                ", msgTimeout=" + msgTimeout +
                ", warnWhenNotUsingTls=" + warnWhenNotUsingTls +
                '}';
    }

}
