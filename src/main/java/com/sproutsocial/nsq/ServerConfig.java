package com.sproutsocial.nsq;

/**
 * nsqd settings that can't be changed by the client.
 * returned in response to IDENTIFY command.
 */
class ServerConfig extends Config {

    private String version;
    private Integer maxRdyCount;
    private Integer maxMsgTimeout;
    private Integer maxDeflateLevel;
    private Boolean authRequired;

    //region accessors
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Integer getMaxRdyCount() {
        return maxRdyCount;
    }

    public void setMaxRdyCount(Integer maxRdyCount) {
        this.maxRdyCount = maxRdyCount;
    }

    public Integer getMaxMsgTimeout() {
        return maxMsgTimeout;
    }

    public void setMaxMsgTimeout(Integer maxMsgTimeout) {
        this.maxMsgTimeout = maxMsgTimeout;
    }

    public Integer getMaxDeflateLevel() {
        return maxDeflateLevel;
    }

    public void setMaxDeflateLevel(Integer maxDeflateLevel) {
        this.maxDeflateLevel = maxDeflateLevel;
    }

    public Boolean getAuthRequired() {
        return authRequired;
    }

    public void setAuthRequired(Boolean authRequired) {
        this.authRequired = authRequired;
    }
    //endregion

    @Override
    public String toString() {
        return "ServerConfig{" +
                "version='" + version + '\'' +
                ", maxRdyCount=" + maxRdyCount +
                ", maxMsgTimeout=" + maxMsgTimeout +
                ", maxDeflateLevel=" + maxDeflateLevel +
                ", authRequired=" + authRequired +
                "} " + super.toString();
    }
}
