package com.zhengzy.kafka;

/**
 * <p>收到消息后业务逻辑处理</p>
 * Created by @author zhezhiyong@163.com on 2018/11/23.
 */
@FunctionalInterface
public interface CallbackService {
    /**
     * 处理业务逻辑
     */
    public abstract void run(String json);
}
