package com.lixiang.exercise01.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserBean implements Writable {
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;



    public Long getUpFlow() {
        return upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }



    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    @Override
    public String toString() {
        return "UserBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    public UserBean(){
        super();
    }

    public UserBean(Long upFlow,Long downFlow){
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    /**
     * 重写序列化方法，注意先先出
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        long upFlow = dataInput.readLong();
        long downFlow = dataInput.readLong();
        sumFlow = upFlow + downFlow;
    }
}
