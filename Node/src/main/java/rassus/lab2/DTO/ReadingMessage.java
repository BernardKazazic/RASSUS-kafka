package rassus.lab2.DTO;

import org.json.JSONObject;
import rassus.lab2.Message;
import rassus.lab2.MessageId;

import java.util.HashMap;
import java.util.Map;

public class ReadingMessage extends Message {
    private String nodeId;
    private MessageId messageId;
    private Double no2Reading;

    public ReadingMessage() {
        this.setType("reading");
    }

    public ReadingMessage(JSONObject json) {
        this();
        this.setScalarTime(json.getLong("scalarTime"));
        Map<String, Object> temp = json.getJSONObject("vectorTime").toMap();
        HashMap<String, Integer> temp2 = new HashMap<>();
        for (Map.Entry<String, Object> entry : temp.entrySet()) {
            if(entry.getValue() instanceof String){
                temp2.put(entry.getKey(), Integer.valueOf((String) entry.getValue()));
            }
        }
        this.setVectorTime(temp2);
        this.setMessageId(new MessageId(json.getJSONObject("messageId")));
        this.nodeId = json.getString("nodeId");
        this.no2Reading = json.getDouble("no2Reading");
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }


    public Double getNo2Reading() {
        return no2Reading;
    }

    public void setNo2Reading(Double no2Reading) {
        this.no2Reading = no2Reading;
    }
}
