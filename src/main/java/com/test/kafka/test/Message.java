package com.test.kafka.test;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author CoderZZ
 * @Title: ${FILE_NAME}
 * @Project: kafkatest
 * @Package com.test.kafka.test
 * @description: TODO:一句话描述信息
 * @Version 1.0
 * @create 2018-08-14 0:00
 **/
public class Message implements Serializable{

    private static int id = 0;

    private String messageId = String.valueOf(++id);

    private String messageInfo;

    public String getMessageId() {
        return messageId;
    }

    public String getMessageInfo() {
        return messageInfo;
    }

    public void setMessageInfo(String messageInfo) {
        this.messageInfo = messageInfo;
    }

    @Override
    public String toString(){
        return "{messageId:"+this.messageId+",messageInfo:"+this.messageInfo+"}";
    }
}
