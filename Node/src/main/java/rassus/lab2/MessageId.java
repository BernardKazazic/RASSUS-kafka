package rassus.lab2;

import org.json.JSONObject;

public class MessageId {
    private final long messageNumber;
    private final String nodeId;

    public MessageId(long messageNumber, String nodeId) {
        this.messageNumber = messageNumber;
        this.nodeId = nodeId;
    }

    public MessageId(JSONObject json) {
        this.messageNumber = json.getLong("messageNumber");
        this.nodeId = json.getString("nodeId");
    }

    public long getMessageNumber() {
        return messageNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageId messageId)) return false;

        if (messageNumber != messageId.messageNumber) return false;
        return nodeId.equals(messageId.nodeId);
    }

    @Override
    public int hashCode() {
        int result = (int) (messageNumber ^ (messageNumber >>> 32));
        result = 31 * result + nodeId.hashCode();
        return result;
    }
}
