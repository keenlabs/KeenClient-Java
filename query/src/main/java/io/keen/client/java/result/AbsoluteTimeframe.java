package io.keen.client.java.result;

public class AbsoluteTimeframe {

    private final String start;
    private final String end;

    public AbsoluteTimeframe(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

}
