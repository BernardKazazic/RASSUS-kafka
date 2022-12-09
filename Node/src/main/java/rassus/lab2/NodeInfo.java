package rassus.lab2;

import org.json.JSONObject;

public class NodeInfo {
    private String id;
    private String address;
    private String port;

    public NodeInfo() {
        super();
    }
    public NodeInfo(JSONObject json) {
        this.id = json.getString("id");
        this.address = json.getString("address");
        this.port = json.getString("port");
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
