package com.dataartisans.flinktraining.test;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by guanghui01.rong on 2018/8/21.
 */
public class Service {
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void read() {
        try {
            try {
                lock.readLock().lock();
                System.out.println("获得读锁" + Thread.currentThread().getName()
                        + " " + System.currentTimeMillis());
                Thread.sleep(10000);
            } finally {
                lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
