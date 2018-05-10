package com.asiainfo.utils;

import java.sql.Types;

/**
 * @author H
 * @desc
 * @date 2018/5/9
 */
public class TypeUtils {

    public static String getSqlType(int javaType) {

        StringBuffer result = new StringBuffer();
        switch (javaType) {
            case Types.VARCHAR: {
                result.append("VARCHAR(50)");
                break;
            }
            case Types.INTEGER: {
                result.append("INTEGER");
                break;
            }
            case Types.DATE: {
                result.append("TIMESTAMP");
                break;
            }
            case Types.TIMESTAMP: {
                result.append("TIMESTAMP");
                break;
            }
            case Types.SMALLINT: {
                result.append("SMALLINT");
                break;
            }
            default:
        }
        return result.toString();
    }
}
