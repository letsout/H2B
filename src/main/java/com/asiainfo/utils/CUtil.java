package com.asiainfo.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.Map;

/**
 * Created by lhy on 2017/7/14.
 */
public class CUtil {
    /**
     * 获取异常的堆栈信息
     *
     * @param t
     * @return
     */
    public static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try {
            t.printStackTrace(pw);
            return sw.toString();
        } finally {
            pw.close();
        }
    }


    /**
     * @param rs
     * @param name
     * @return
     */
    public static String getRsValue(Map<String, Integer> columnMap, ResultSet rs, String name) {
        try {
            Integer columnType = columnMap.get(name);
            if (columnType != null) {
                StringBuffer result = new StringBuffer();
                switch (columnType.intValue()) {
                    case Types.VARCHAR: {
                        result.append(rs.getString(name));
                        break;
                    }
                    case Types.INTEGER: {
                        result.append(rs.getInt(name));
                        break;
                    }
                    case Types.CLOB: {
                        Clob clob = rs.getClob(name);
                        result.append(clob.getSubString(1, (int) clob.length()));
                        break;
                    }
                    case Types.DATE: {
                        result.append(rs.getDate(name).toString());
                        break;
                    }
                    case Types.BLOB: {
                        Blob blob = rs.getBlob(name);
                        result.append(new String(blob.getBytes(1, (int) blob.length()), "GBK"));
                        break;
                    }
                    case Types.TIMESTAMP: {
                        Timestamp timestamp = rs.getTimestamp(name);
                        result.append(timestamp.toString());
                        break;
                    }
                    case Types.SMALLINT: {
                        result.append(rs.getInt(name));
                        break;
                    }
                    default:
                }
                return result.toString();
            } else {
                return "";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}
