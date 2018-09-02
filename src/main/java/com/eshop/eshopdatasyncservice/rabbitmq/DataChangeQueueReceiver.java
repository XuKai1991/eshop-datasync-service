package com.eshop.eshopdatasyncservice.rabbitmq;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.eshop.eshopdatasyncservice.cons.Constants;
import com.eshop.eshopdatasyncservice.service.ProductFeignClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 数据同步服务，就是获取各种原子数据的变更消息
 * （1）然后通过spring cloud fegion调用eshop-product-service服务的各种接口， 获取数据
 * （2）将原子数据在redis中进行增删改
 * （3）将维度数据变化消息写入rabbitmq中另外一个queue，供数据聚合服务来消费
 */
@Component
@RabbitListener(queues = "data-change-queue")
@Slf4j
public class DataChangeQueueReceiver {

    @Autowired
    private ProductFeignClient productFeignClient;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private RabbitMQSender rabbitMQSender;

    public DataChangeQueueReceiver() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("DataChangeQueueReceiver").build();
        ThreadPoolExecutor threadExecutor = new ThreadPoolExecutor(
                1,
                1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                namedThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());
        threadExecutor.submit(new SendThread());
    }

    /**
     * 聚合product消息的去重队列
     */
    private Set<String> dimDataMessageSet = Collections.synchronizedSet(new HashSet<>());

    private List<Long> brandDataChangeIdList = new ArrayList<>();

    @RabbitHandler
    public void process(String content) {
        log.info("商品同步服务消费消息 : " + content);
        JSONObject jsonObject = JSONObject.parseObject(content);
        // 先获取data_type
        String dataType = jsonObject.getString(Constants.RQ_DATA_TYPE);
        if ("brand".equals(dataType)) {
            processBrandDataChangeMessage(jsonObject);
        } else if ("category".equals(dataType)) {
            processCategoryDataChangeMessage(jsonObject);
        } else if ("product".equals(dataType)) {
            processProductDataChangeMessage(jsonObject);
        } else if ("product_intro".equals(dataType)) {
            processProductIntroDataChangeMessage(jsonObject);
        } else if ("product_property".equals(dataType)) {
            processProductPropertyDataChangeMessage(jsonObject);
        } else if ("product_specification".equals(dataType)) {
            processProductSpecificationDataChangeMessage(jsonObject);
        }
    }

    private void processBrandDataChangeMessage(JSONObject jsonObject) {
        String eventType = jsonObject.getString(Constants.RQ_EVENT_TYPE);
        if (Constants.RQ_EVENT_TYPE_ADD.equals(eventType) || Constants.RQ_EVENT_TYPE_UPDATE.equals(eventType)) {
            Long id = jsonObject.getLong(Constants.RQ_ID);
            brandDataChangeIdList.add(id);
            if (brandDataChangeIdList.size() >= 2) {
                String brandDataChangeIds = "";
                for (Long brandDataChangeId : brandDataChangeIdList) {
                    brandDataChangeIds += ("," + brandDataChangeId);
                }
                String brandDataChangeIdStr = brandDataChangeIds.replaceFirst(",", "");
                JSONArray brandJsonArray = JSONObject.parseArray(productFeignClient.findBrandByIds(brandDataChangeIdStr));
                for (int i = 0; i < brandJsonArray.size(); i++) {
                    JSONObject brandJsonObject = brandJsonArray.getJSONObject(i);
                    Long brandId = brandJsonObject.getLong(Constants.RQ_ID);
                    redisTemplate.opsForValue().set(Constants.REDIS_BRAND_KEY + brandId, brandJsonObject.toJSONString());
                    rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, "{\"dim_type\": \"brand\", \"id\": " + brandId + "}");
                }
                brandDataChangeIdList.clear();
            }
        } else if (Constants.RQ_EVENT_TYPE_DELETE.equals(eventType)) {
            Long id = jsonObject.getLong(Constants.RQ_ID);
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_BRAND_KEY + id);
            rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, "{\"dim_type\": \"brand\", \"id\": " + id + "}");
        }
    }

    private void processCategoryDataChangeMessage(JSONObject jsonObject) {
        Long id = jsonObject.getLong(Constants.RQ_ID);
        String eventType = jsonObject.getString(Constants.RQ_EVENT_TYPE);
        if (Constants.RQ_EVENT_TYPE_ADD.equals(eventType) || Constants.RQ_EVENT_TYPE_UPDATE.equals(eventType)) {
            String jsonCategory = productFeignClient.findCategoryById(id);
            redisTemplate.opsForValue().set(Constants.REDIS_CATEGORY_KEY + id, jsonCategory);
        } else if (Constants.RQ_EVENT_TYPE_DELETE.equals(eventType)) {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_CATEGORY_KEY + id);
        }
        rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, "{\"dim_type\": \"category\", \"id\": " + id + "}");
    }

    private void processProductDataChangeMessage(JSONObject jsonObject) {
        Long id = jsonObject.getLong(Constants.RQ_ID);
        String eventType = jsonObject.getString(Constants.RQ_EVENT_TYPE);
        if (Constants.RQ_EVENT_TYPE_ADD.equals(eventType) || Constants.RQ_EVENT_TYPE_UPDATE.equals(eventType)) {
            String jsonProduct = productFeignClient.findProductById(id);
            redisTemplate.opsForValue().set(Constants.REDIS_PRODUCT_KEY + id, jsonProduct);
        } else if (Constants.RQ_EVENT_TYPE_DELETE.equals(eventType)) {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_PRODUCT_KEY + id);
        }
        // 使用消息去重队列
        // rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, "{\"dim_type\": \"product\", \"id\": " + id + "}");
        dimDataMessageSet.add("{\"dim_type\": \"product\", \"id\": " + id + "}");
    }

    private void processProductIntroDataChangeMessage(JSONObject jsonObject) {
        Long id = jsonObject.getLong(Constants.RQ_ID);
        Long productId = jsonObject.getLong(Constants.RQ_PRODUCT_ID);
        String eventType = jsonObject.getString(Constants.RQ_EVENT_TYPE);
        if (Constants.RQ_EVENT_TYPE_ADD.equals(eventType) || Constants.RQ_EVENT_TYPE_UPDATE.equals(eventType)) {
            String jsonProductIntro = productFeignClient.findProductIntroById(id);
            redisTemplate.opsForValue().set(Constants.REDIS_PRODUCT_INTRO_KEY + productId, jsonProductIntro);
        } else if (Constants.RQ_EVENT_TYPE_DELETE.equals(eventType)) {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_PRODUCT_INTRO_KEY + productId);
        }
        rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, "{\"dim_type\": \"product_intro\", \"id\": " + productId + "}");
    }

    private void processProductPropertyDataChangeMessage(JSONObject jsonObject) {
        Long id = jsonObject.getLong(Constants.RQ_ID);
        Long productId = jsonObject.getLong(Constants.RQ_PRODUCT_ID);
        String eventType = jsonObject.getString(Constants.RQ_EVENT_TYPE);
        if (Constants.RQ_EVENT_TYPE_ADD.equals(eventType) || Constants.RQ_EVENT_TYPE_UPDATE.equals(eventType)) {
            String jsonProductProperty = productFeignClient.findProductPropertyById(id);
            redisTemplate.opsForValue().set(Constants.REDIS_PRODUCT_PROPERTY_KEY + productId, jsonProductProperty);
        } else if (Constants.RQ_EVENT_TYPE_DELETE.equals(eventType)) {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_PRODUCT_PROPERTY_KEY + productId);
        }
        // 使用消息去重队列
        // rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, "{\"dim_type\": \"product\", \"id\": " + productId + "}");
        dimDataMessageSet.add("{\"dim_type\": \"product\", \"id\": " + productId + "}");
    }

    private void processProductSpecificationDataChangeMessage(JSONObject jsonObject) {
        Long id = jsonObject.getLong(Constants.RQ_ID);
        Long productId = jsonObject.getLong(Constants.RQ_PRODUCT_ID);
        String eventType = jsonObject.getString(Constants.RQ_EVENT_TYPE);
        if (Constants.RQ_EVENT_TYPE_ADD.equals(eventType) || Constants.RQ_EVENT_TYPE_UPDATE.equals(eventType)) {
            String jsonProductProperty = productFeignClient.findProductSpecificationById(id);
            redisTemplate.opsForValue().set(Constants.REDIS_PRODUCT_SPECIFICATION_KEY + productId, jsonProductProperty);
        } else if (Constants.RQ_EVENT_TYPE_DELETE.equals(eventType)) {
            redisTemplate.opsForValue().getOperations().delete(Constants.REDIS_PRODUCT_SPECIFICATION_KEY + productId);
        }
        // 使用消息去重队列
        // rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, "{\"dim_type\": \"product\", \"id\": " + productId + "}");
        dimDataMessageSet.add("{\"dim_type\": \"product\", \"id\": " + productId + "}");
    }

    private class SendThread extends Thread {
        @Override
        public void run() {
            while (true) {
                if (dimDataMessageSet.size() != 0) {
                    for (String dimDataMessage : dimDataMessageSet) {
                        rabbitMQSender.send(RabbitQueue.AGGR_DATA_CHANGE_QUEUE, dimDataMessage);
                    }
                    dimDataMessageSet.clear();
                }
                try {
                    // 间隔1秒
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
