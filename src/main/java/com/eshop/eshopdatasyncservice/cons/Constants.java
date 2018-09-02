package com.eshop.eshopdatasyncservice.cons;

/**
 * @author: Xukai
 * @description:
 * @createDate: 2018/8/28 11:04
 * @modified By:
 */
public class Constants {

    /* RabbitMQ Key */
    public static String RQ_ID = "id";
    public static String RQ_PRODUCT_ID = "product_id";
    public static String RQ_DATA_TYPE = "data_type";
    public static String RQ_EVENT_TYPE = "event_type";
    public static String RQ_EVENT_TYPE_ADD = "add";
    public static String RQ_EVENT_TYPE_UPDATE = "update";
    public static String RQ_EVENT_TYPE_DELETE = "delete";

    /* Redis Key */
    public static String REDIS_BRAND_KEY = "brand:";
    public static String REDIS_CATEGORY_KEY = "category:";
    public static String REDIS_PRODUCT_KEY = "product:";
    public static String REDIS_PRODUCT_INTRO_KEY = "product_intro:";
    public static String REDIS_PRODUCT_PROPERTY_KEY = "product_property:";
    public static String REDIS_PRODUCT_SPECIFICATION_KEY = "product_specification:";


}
