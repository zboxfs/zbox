package io.zbox;

public enum Cipher {
    XCHACHA(0),
    AES(1);

    private final int id;

    Cipher(int id) { this.id = id;  }
    public int getValue() { return id;  }
}

