package com.simplekafka.broker;

public class ProduceResult {
    private final long offset;
    private final String error;

    public ProduceResult(long offset, String error) {
        this.offset = offset;
        this.error = error;
    }
    public long getOffset() {
        return offset;
    }
    public String getError() {
        return error;
    }
    public boolean isSuccess() {
        return error == null;
    }
}
