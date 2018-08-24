package com.dataartisans.flinktraining.test;

/**
 * Created by guanghui01.rong on 2018/8/21.
 */
public class ThreadMain {

    public static void main(String[] args) {

        Service service = new Service();

        ThreadA a = new ThreadA(service);
        a.setName("I am Thread A");

        ThreadB b = new ThreadB(service);
        b.setName("I am Thread B");

        a.start();
        b.start();
    }
}
