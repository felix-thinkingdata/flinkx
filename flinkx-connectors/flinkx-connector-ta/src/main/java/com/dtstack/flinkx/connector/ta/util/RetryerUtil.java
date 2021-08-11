package com.dtstack.flinkx.connector.ta.util;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.rholder.retry.*;
import com.google.common.base.Throwables;

/**
 * Created by zhoujin on 2018/2/9.
 */
public class RetryerUtil {
    private static final Logger logger = LoggerFactory.getLogger(RetryerUtil.class);

    public static Retryer initRetryer(){
        Retryer retryer = RetryerBuilder.newBuilder().retryIfException()
                .withWaitStrategy(WaitStrategies.fibonacciWait(500,5, TimeUnit.MINUTES))
                .withStopStrategy(StopStrategies.neverStop())
                .withRetryListener(new RetryListener() {
                    @Override
                    public <V> void onRetry(Attempt<V> attempt) {
                        if (attempt.hasException()){
                            logger.error(Throwables.getStackTraceAsString(attempt.getExceptionCause()));
                            logger.info("开始进行失败重试，重试次数：" + attempt.getAttemptNumber() + "， 距离第一次失败时间：" + attempt.getDelaySinceFirstAttempt() / 1000 + "秒");
                        }
                    }
                })
                .build();
        return retryer;
    }

    public static Retryer initRetryerByTimes(int retryTimes,long sleepSeconds){
        Retryer retryer = RetryerBuilder.newBuilder().retryIfException()
                .withWaitStrategy(WaitStrategies.fixedWait(sleepSeconds,TimeUnit.SECONDS))
                .withStopStrategy(StopStrategies.stopAfterAttempt(retryTimes))
                .withRetryListener(new RetryListener() {
                    @Override
                    public <V> void onRetry(Attempt<V> attempt) {
                        if (attempt.hasException()){
                            logger.error(Throwables.getStackTraceAsString(attempt.getExceptionCause()));
                            logger.info("开始进行失败重试，重试次数：" + attempt.getAttemptNumber() + "， 距离第一次失败时间：" + attempt.getDelaySinceFirstAttempt() / 1000 + "秒");
                        }
                    }
                })
                .build();
        return retryer;
    }


}
