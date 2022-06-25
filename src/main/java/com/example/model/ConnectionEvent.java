package com.example.model;

public class ConnectionEvent {
    private String serial;
    private String status;
    private String endpoint;

    public ConnectionEvent(String serial, String status, String endpoint) {
        this.serial = serial;
        this.status = status;
        this.endpoint = endpoint;
    }

    public String getSerial() {
        return serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String toString() {
        return "ConnectionEvent{" +
                "serial='" + serial + '\'' +
                ", status='" + status + '\'' +
                ", endpoint='" + endpoint + '\'' +
                '}';
    }
}
