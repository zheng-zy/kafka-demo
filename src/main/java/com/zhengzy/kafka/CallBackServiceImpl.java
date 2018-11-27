package com.zhengzy.kafka;

import com.sun.scenario.effect.Offset;

/**
 * <p></p>
 * Created by zhezhiyong@163.com on 2018/11/27.
 */
public class CallBackServiceImpl implements CallbackService {
    @Override
    public void run(String json) {
        String[] strings = json.split(":");

        if (strings.length == 2) {
            System.out.println("Offset = " + strings[0]);
            if (strings[1].length() > 0) {
                System.out.println("处理具体业务 = " + strings[1]);
            }
        }
    }
}
