package com.simplekafka.broker.MethodReturnTypes;

public class FetchResponse {
    private final byte [][] messages;
    private final String error;
    public FetchResponse(byte [][] messages, String error) {
        this.messages = messages;
        this.error = error;
    }
    public byte [][] getMessages() {
        return messages;
    }
    public String getError() {
        return error;
    }
    public boolean isSuccess() {
        return error == null;
    }
    
}
