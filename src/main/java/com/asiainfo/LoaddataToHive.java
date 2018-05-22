package com.asiainfo;

import com.asiainfo.service.IOperationsOn199Service;
import com.asiainfo.utils.CUtil;
import com.asiainfo.utils.TypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by c on 2017/1/5.
 * 执行load data input 'xxx' into table table_xxxx语句
 * beeline不支持nohup 方式运行
 */
public class LoaddataToHive {

    private Connection conn = null;

    public Connection getConnection(String url) throws Exception {

        Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();

        this.conn = DriverManager.getConnection(url, "", "");

        return conn;
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("java.security.krb5.conf", "conf/krb5.conf");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(new String[]{"classpath*:spring-hive.xml"});
        IOperationsOn199Service operationsOn199Service = (IOperationsOn199Service) applicationContext.getBean("operationsOn199Service");

        //String url = args[1];
        String tabName = args[0];
        String tablenName199 = args[1];
        String condition = args[2];
        int tbNameLength = args[0].length();
        StringBuilder url = new StringBuilder(
                "jdbc:hive2://10.113.246.10:24002,10.113.246.8:24002,10.113.246.9:24002/");
        url.append(";serviceDiscoveryMode=").append("zookeeper");
        url.append(";user.principal=").append("yx_census@FI1.COM");
        url.append(";user.keytab=").append("conf/user.keytab");
        url.append(";sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.fi1.com@FI1.COM;");
        System.out.println("url:" + url);
        LoaddataToHive loader = new LoaddataToHive();
        System.out.println("Connecting...");
        Statement st = loader.getConnection(url.toString()).createStatement();
        
        String queryStructSql;
        if(StringUtils.isNotBlank(condition)){
            queryStructSql = "select * from " + tabName+" "+condition;
        }else {
            queryStructSql = "select * from " + tabName;
        }

        ResultSet rs = st.executeQuery(queryStructSql);
        ResultSetMetaData rsmd = rs.getMetaData();

        System.out.println("====================================");

        int colCount = rsmd.getColumnCount();
        System.out.println("tabName In This Batch:" + tabName + " ColumnCount:" + colCount);

        System.out.println("====================================");

        System.out.println("===================Create table on 199===================");
        operationsOn199Service.createTab(createSql(rsmd,tabName,tablenName199, colCount));

        /**
         * 此Map里面存储这张表的每个字段名和字段类型(int)->Types这个类是Sql和Java类型对应起来的类。
         */
        Map<String, Integer> metaMap = new HashMap();
        for (int index = 1; index <= colCount; index++) {
            metaMap.put(rsmd.getColumnName(index).substring(tbNameLength + 1), rsmd.getColumnType(index));
        }

        //获取插入语句模版
        String insertSql = insertSql(metaMap, tablenName199);

        List<Map<String, ?>> mapList = new ArrayList<>();
        Map<String, String> listMap;
        try {
            ResultSet resultSet = st.executeQuery(queryStructSql);
            /**
             * 根据查询的结果，遍历每一行数据。
             */
            while (resultSet.next()) {
                listMap = new HashMap();
                /**
                 * 循环遍历每一行的数据,将每一行的colName和colValue按照Map存储
                 */
                for (int i = 1; i <= colCount; i++) {

                    String colName = rsmd.getColumnName(i);
                    listMap.put(colName.substring(tbNameLength + 1), CUtil.getRsValue(metaMap, resultSet, colName, tbNameLength));
                }
                //每一行数据存在一个新Map中（防止引用传递）,每一行循环完后将这个Map add到list中。
                mapList.add(listMap);

                /**
                 * 当list中有50000行数据的时候进行入表操作，入表操作为异步，
                 * 每次入表时给list一个新的指针和地址，防止引用传递。
                 */
                if (mapList.size() == 50000) {
                    operationsOn199Service.insertIntoNewTab(mapList, insertSql);

                    mapList = new ArrayList<>();
                }
            }
            //整个表数据循环完后，如果list中还有数据，就要再执行一次入表操作。（最后一批次数据不满50000）
            if (0 != mapList.size()) {
                operationsOn199Service.insertIntoNewTab(mapList, insertSql);
            }

        } catch (Exception e) {
            throw new Exception();
        } finally {
            loader.close();
        }

    }

    /**
     * 拼接建表Sql
     *
     * @param rsmd      表结构信息
     * @param tableName
     * @param colCount
     * @return
     * @throws SQLException
     */
    public static String createSql(ResultSetMetaData rsmd,String tableName, String tableName199, int colCount) throws SQLException {

        int tbNameLength = tableName.length();
        StringBuffer createSql = new StringBuffer("create table ");
        createSql.append(tableName199).append(" (");

        for (int i = 1; i <= colCount; i++) {
            createSql.append(rsmd.getColumnName(i).substring(tbNameLength + 1)).append(" ").append(TypeUtils.getSqlType(rsmd.getColumnType(i)))
                    .append(",");
        }

        //因为上面多一个逗号，下面的以此类推。
        String tmpSql = createSql.substring(0, createSql.length() - 1).toString();

        StringBuffer createSql2 = new StringBuffer(tmpSql);
        createSql2.append(") ").append("distribute by hash(").append(rsmd.getColumnName(1).substring(tbNameLength + 1)).append(") ");
        createSql2.append("in tbs_app_imcd not logged initially");

        System.out.println("CreateTableSql:" + createSql2.toString());
        return createSql2.toString();
    }

    public static String insertSql(Map<String, Integer> metaMap, String tabName) {

        StringBuffer insertSql = new StringBuffer("insert into ");
        insertSql.append(tabName).append(" ( ");

        metaMap.forEach((k, v) -> {

            insertSql.append(k).append(",");
        });
        String tmpSql = insertSql.substring(0, insertSql.length() - 1).toString();

        StringBuffer insertSql2 = new StringBuffer(tmpSql);

        insertSql2.append(" ) values ( ");
        metaMap.forEach((k, v) -> {

            insertSql2.append(":" + k).append(",");
        });
        String tmpSql2 = insertSql2.substring(0, insertSql2.length() - 1).toString();

        StringBuffer insertSql3 = new StringBuffer(tmpSql2);

        insertSql3.append(" )");

        System.out.println("insertSql:" + insertSql3.toString());

        return insertSql3.toString();
    }
}
