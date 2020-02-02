package com.lixiang.exercise01.mr;


import com.lixiang.exercise01.bean.UserBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"输入文件路径","输出文件路径"};
        Configuration conf = new Configuration();
        //1 获取job对象
        Job job = Job.getInstance(conf);
        //2 设置jar路径
        job.setJarByClass(driver.class);
        //3 关联mapper和reducer
        job.setMapperClass(flowMapper.class);
        job.setReducerClass(flowReduce.class);
        //4 设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserBean.class);
        //5 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserBean.class);
        //6 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }
}
