package org.awsutils.sqs.listener;

import java.util.TimerTask;

class DefaultTimerTask extends TimerTask {
    private final Runnable task;

    DefaultTimerTask(Runnable task) {
        this.task = task;
    }

    @Override
    public void run() {
        task.run();
    }
}
