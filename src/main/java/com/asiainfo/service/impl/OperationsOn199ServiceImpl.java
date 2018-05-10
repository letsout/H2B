package com.asiainfo.service.impl;

import com.asiainfo.dao.IOperationsOn199Dao;
import com.asiainfo.service.IOperationsOn199Service;
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


    @Autowired
    private IOperationsOn199Dao operationsOn199Dao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    public void createTab(String sql) {

        jdbcTemplate.update(sql, new HashMap<>(16));

    }

    @Override
    @Async
    public void insertIntoNewTab(List<Map<String, ?>> mapList, String insertSql) {

        long begin = System.currentTimeMillis();

        Map<String, ?>[] map = new HashMap[mapList.size()];

        jdbcTemplate.batchUpdate(insertSql, SqlParameterSourceUtils.createBatch(mapList.toArray(map)));

        String timeW = String.valueOf(System.currentTimeMillis() - begin);
        System.out.println("JDBC: " + timeW + " ms");

    }
}
