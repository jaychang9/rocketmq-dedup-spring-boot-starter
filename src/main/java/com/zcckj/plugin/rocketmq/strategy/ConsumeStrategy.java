package com.zcckj.plugin.rocketmq.strategy;


import java.util.Map;
import java.util.function.BiFunction;


public interface ConsumeStrategy {
     <T> boolean invoke(BiFunction<T, Map<String, Object>, Boolean> consumeCallback, T message, Map<String, Object> extMap);
}

