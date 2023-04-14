package com.atguigu.kafkastream;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: MovieRecommendSystem
 * Package: com.atguigu.kafkastream
 * Version: 1.0
 * <p>
 * Created by wushengran on 2019/4/4 10:49
 */

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @ClassName: LogProcessor
 * @Description:
 * @Author: wushengran on 2019/4/4 10:49
 * @Version: 1.0
 */
public class LogProcessor implements Processor<byte[], byte[]>{

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        // 把收集到的日志信息用string表示
        String input = new String(line);
        // 根据前缀MOVIE_RATING_PREFIX:从日志信息中提取评分数据
        if( input.contains("MOVIE_RATING_PREFIX:") ){
            System.out.println("movie rating data coming!>>>>>>>>>>>" + input);

            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();
            context.forward( "logProcessor".getBytes(), input.getBytes() );
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
