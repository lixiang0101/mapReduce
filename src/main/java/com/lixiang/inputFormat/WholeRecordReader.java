package com.lixiang.inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader<T, T1> extends RecordReader<Text, BytesWritable> {

    //切片
    FileSplit split;
    Configuration conf;

    //初始化输出的k和v
    Text k;
    BytesWritable v;

    //
    boolean isProgress = true;

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        conf = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        //核心业务
        if (isProgress){
            byte[] buffer = new byte[(int) split.getLength()];
            //1 获取fs对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);
            //2 获取输入流
            FSDataInputStream fis = fs.open(path);
            //3 拷贝
            IOUtils.readFully(fis,buffer,0, buffer.length);
            //4 封装成v
            v.set(buffer,0,buffer.length);
            //5 封装成k
            k.set(path.toString());
            //6 关闭资源
            IOUtils.closeStream(fis);

            isProgress = false;
            return true;
        }
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {

        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    public float getProgress() throws IOException, InterruptedException {
        //暂时不考虑
        return 0;
    }

    public void close() throws IOException {
        //暂时不考虑
    }
}
