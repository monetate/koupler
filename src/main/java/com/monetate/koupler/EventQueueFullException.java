package com.monetate.koupler;

public class EventQueueFullException extends Exception {
    private static final long serialVersionUID = 1L;
    private int size = -1;
    
    public EventQueueFullException(int size){
        this.size = size;
    }

    public int getSize() {
        return size;
    }
}
