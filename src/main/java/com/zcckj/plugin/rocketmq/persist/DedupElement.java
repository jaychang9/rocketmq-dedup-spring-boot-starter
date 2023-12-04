package com.zcckj.plugin.rocketmq.persist;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@AllArgsConstructor
public class DedupElement {
    /**
     * 应用名，可取spring.application.name
     */
    private String application;
    /**
     * 消息Topic
     */
    private String topic;
    /**
     * 消息Tag
     */
    private String tag;
    /**
     * 消息Key
     */
    private String msgUniqKey;

}
