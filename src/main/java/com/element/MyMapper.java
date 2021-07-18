package com.element;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MyMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phoneNumber = fields[1];
        long upFlow = Long.parseLong(fields[7]);
        long downFlow = Long.parseLong(fields[8]);

        context.write(new Text(phoneNumber), new FlowBean(upFlow, downFlow));
    }
}
