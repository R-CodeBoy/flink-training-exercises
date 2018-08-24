package com.dataartisans.flinktraining.test;

/**
 * Created by guanghui01.rong on 2018/8/21.
 */
public class ThreadA  extends Thread{
    private Service service;

    public ThreadA(Service service) {
        super();
        this.service = service;
    }

    @Override
    public void run() {
        service.read();
    }
}
