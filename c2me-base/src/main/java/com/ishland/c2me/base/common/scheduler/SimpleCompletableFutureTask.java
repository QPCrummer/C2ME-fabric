package com.ishland.c2me.base.common.scheduler;

import com.ishland.flowsched.executor.SimpleTask;

import java.util.concurrent.CompletableFuture;

public class SimpleCompletableFutureTask extends SimpleTask {

    private final CompletableFuture future;

    public SimpleCompletableFutureTask(CompletableFuture wrapped, int priority) {
        super((Runnable) wrapped, priority);
        this.future = wrapped;
    }

    public CompletableFuture getFuture() {
        return this.future;
    }
}
