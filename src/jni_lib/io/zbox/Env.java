package io.zbox;

public final class Env {
    public static native int init();

    static {
        System.loadLibrary("zbox");
    }
}
