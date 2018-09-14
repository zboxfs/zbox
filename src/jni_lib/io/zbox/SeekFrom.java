package io.zbox;

public enum SeekFrom {
    START(0),
    CURRENT(1),
    END(2);

    private final int id;

    SeekFrom(int id) { this.id = id;  }
    public int getValue() { return id;  }
}
