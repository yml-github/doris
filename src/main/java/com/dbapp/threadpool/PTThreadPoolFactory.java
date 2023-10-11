package com.dbapp.threadpool;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 压测线程池配置
 *
 * @author yangmenglong
 * @date 2023/10/11
 */
@Configuration
public class PTThreadPoolFactory {

    @Bean(name = "doris")
    public ExecutorService dorisTaskExecutor() {
        return new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10), new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,
                        "doris-" + threadNumber.getAndIncrement());
            }
        });
    }

    @Bean(name = "es")
    public ExecutorService esTaskExecutor() {
        return new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10), new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,
                        "es-" + threadNumber.getAndIncrement());
            }
        });
    }
}
