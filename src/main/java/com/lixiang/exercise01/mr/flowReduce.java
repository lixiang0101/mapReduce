package com.lixiang.exercise01.mr;

import com.lixiang.exercise01.bean.UserBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class flowReduce extends Reducer<Text, UserBean, Text,UserBean> {
    /**
     * 从map中把key相同的数据传进来，然后做求和
     */
    UserBean v = new UserBean();
    @Override
    protected void reduce(Text key, Iterable<UserBean> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        //累加求和
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (UserBean value : values) {
            sumUpFlow = sumUpFlow + value.getUpFlow();
            sumDownFlow = sumDownFlow + value.getDownFlow();
        }
        v.setUpFlow(sumUpFlow);
        v.setDownFlow(sumDownFlow);
        v.setSumFlow(sumUpFlow + sumDownFlow);
        //写出
        context.write(key,v);
    }
}
