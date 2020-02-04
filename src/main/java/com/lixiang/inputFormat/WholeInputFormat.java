package com.lixiang.inputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义inputFormat类
 * 把文件切片的路径作为k，文件的内容作为v
 */
public class WholeInputFormat extends FileInputFormat<Text, BytesWritable> {

    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        RecordReader<Text, BytesWritable> wholeRecordReader = new WholeRecordReader<Text, BytesWritable>();
        return wholeRecordReader;
    }
}
