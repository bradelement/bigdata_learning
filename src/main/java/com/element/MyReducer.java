package com.element;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean v: values) {
            sumUpFlow += v.getUpFlow();
            sumDownFlow += v.getDownFlow();
        }
        context.write(key, new FlowBean(sumUpFlow, sumDownFlow));
    }
}
