package com.asiainfo.service.impl;

import com.asiainfo.LoaddataToHive;
import com.asiainfo.dao.IOperationsOn199Dao;
import com.asiainfo.service.IOperationsOn199Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/5/8 0008.
 */
@Service("operationsOn199Service")
public class OperationsOn199ServiceImpl implements IOperationsOn199Service {

    private final static Logger logger = LoggerFactory.getLogger(LoaddataToHive.class);

    @Autowired
    private IOperationsOn199Dao operationsOn199Dao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    public void createTab(String sql) {

        operationsOn199Dao.createTab(sql);

    }

    @Override
    @Async
    public void insertIntoNewTab(List<Map<String, ?>> mapList, String insertSql) {

        long begin = System.currentTimeMillis();

        Map<String, ?>[] map = new HashMap[mapList.size()];

        jdbcTemplate.batchUpdate(insertSql, SqlParameterSourceUtils.createBatch(mapList.toArray(map)));

        logger.info("JDBC:{}ms", System.currentTimeMillis() - begin);

    }
}
