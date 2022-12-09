package rassus.lab2.DTO;

import org.json.JSONObject;
import rassus.lab2.Message;
import rassus.lab2.MessageId;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class AckMessage extends Message {
    private MessageId messageId;

    public AckMessage() {
        this.setType("ack");
    }

    public AckMessage(JSONObject json) {
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
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

}