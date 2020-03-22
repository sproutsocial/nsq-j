package com.sproutsocial.nsq;

/**
 * Helper class for observing Client behavior in unit tests.
 */
class ObservedConnectionClient extends Client {

    private int openConnections = 0;
    private boolean connectionClosedCalled;

    @Override
    void addSubConnection(SubConnection subCon) {
        super.addSubConnection(subCon);
        this.openConnections++;
    }

    @Override
    void connectionClosed(SubConnection closedCon) {
        super.connectionClosed(closedCon);
        this.openConnections--;
        this.connectionClosedCalled = true;
    }

    boolean allSubConnectionsClosed() {
        return this.openConnections == 0;
    }

    boolean connectionClosedCalled() {
        return this.connectionClosedCalled;
    }
}
