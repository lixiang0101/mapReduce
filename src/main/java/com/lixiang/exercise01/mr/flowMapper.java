package com.lixiang.exercise01.mr;


import com.lixiang.exercise01.bean.UserBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class flowMapper extends Mapper<LongWritable,Text,Text,UserBean>{
    /**
     * 传入的key是每行的偏移量
     */

    Text k = new Text();
    UserBean v = new UserBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //把行转成String
        String line = value.toString();
        //切分
        String[] fields = line.split("\t");
        //封装成bean对象
        k.set(fields[1]);
        long downFlow = Long.parseLong(fields[fields.length - 3]);
        long upFlow = Long.parseLong(fields[fields.length-2]);
        v.setDownFlow(downFlow);
        v.setUpFlow(upFlow);
        v.setSumFlow(downFlow + upFlow);
        //写出
        context.write(k,v);

    }
}
