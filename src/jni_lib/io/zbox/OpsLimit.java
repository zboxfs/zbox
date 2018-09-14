package io.zbox;

public enum OpsLimit {
    INTERACTIVE(0),
    MODERATE(1),
    SENSITIVE(2);

    private final int id;

    OpsLimit(int id) { this.id = id;  }
    public int getValue() { return id;  }
}
