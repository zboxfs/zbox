package io.zbox;

public enum MemLimit {
    INTERACTIVE(0),
    MODERATE(1),
    SENSITIVE(2);

    private final int id;

    MemLimit(int id) { this.id = id;  }
    public int getValue() { return id;  }
}
