package com.zcckj.plugin.rocketmq.core;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author jaychang
 * @description 持久化类型
 * @date 2023/12/4
 **/
@Getter
@AllArgsConstructor
public enum  PersistTypeEnum {

    DB(0,"DB Persist"),
    REDIS(0,"Redis Persist");

    private final Integer code;
    private final String label;
}
