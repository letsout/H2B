package com.asiainfo.service;

import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/5/8 0008.
 */
public interface IOperationsOn199Service {
    /**
     * 在199上创建数据表
     *
     * @param sql
     */
    void createTab(String sql);

    /**
     * 数据异步插入:50000/b
     *
     * @param mapList
     */
    void insertIntoNewTab(List<Map<String, ?>> mapList, String insertSql);
}
