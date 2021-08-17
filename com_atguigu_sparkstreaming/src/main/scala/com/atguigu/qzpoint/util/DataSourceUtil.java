package com.atguigu.qzpoint.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 德鲁伊连接池
 *
 * @author atguigu
 */
public class DataSourceUtil implements Serializable {
    public static DataSource dataSource = null;

    static {
        try {
            Properties props = new Properties();
            props.setProperty("url", ConfigurationManager.getProperty("jdbc.url"));
            props.setProperty("username", ConfigurationManager.getProperty("jdbc.user"));
            props.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));
            //初始化时建立物理连接的个数。初始化发生在显示调用init方法，或者第一次getConnection时
            props.setProperty("initialSize", "5");
            //最大连接池数量
            props.setProperty("maxActive", "10");
            //最小连接池数量 (maxIdle属性已被弃用)
            props.setProperty("minIdle", "5");
            //获取连接时最大等待时间，单位毫秒。配置了maxWait之后，缺省启用公平锁，并发效率会有所下降，如果需要可以通过配置useUnfairLock属性为true使用非公平锁。
            props.setProperty("maxWait", "60000");
            //配置多久进行一次检测,检测需要关闭的连接 单位毫秒
            props.setProperty("timeBetweenEvictionRunsMillis", "2000");
            //连接保持空闲而不被驱逐的最小时间
            props.setProperty("minEvictableIdleTimeMillis", "600000");
            //用来检测连接是否有效的sql，要求是一个查询语句，常用select 'x'。如果validationQuery为null，testOnBorrow、testOnReturn、testWhileIdle都不会起作用。
            props.setProperty("validationQuery", "select 1");
            // 建议配置为true，不影响性能，并且保证安全性。
            // 申请连接的时候检测，如果空闲时间大于timeBetweenEvictionRunsMillis，执行validationQuery检测连接是否有效。
            props.setProperty("testWhileIdle", "true");
            //申请连接时执行validationQuery检测连接是否有效，做了这个配置会降低性能。
            props.setProperty("testOnBorrow", "false");
            //归还连接时执行validationQuery检测连接是否有效，做了这个配置会降低性能。
            props.setProperty("testOnReturn", "false");
            //连接池中的minIdle数量以内的连接，空闲时间超过minEvictableIdleTimeMillis，则会执行keepAlive操作。
            props.setProperty("keepAlive", "true");
            // 这一项可配可不配，如果不配置druid会根据url自动识别dbType，然后选择相应的driverClassName
            // props.setProperty("driverClassName", "com.mysql.jdbc.Driver");

            // 配置完props之后，使用工厂类创建dataSource
            dataSource = DruidDataSourceFactory.createDataSource(props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  提供获取连接的方法
     */
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 提供关闭资源的方法【connection是归还到连接池】
     */
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement,
                                     Connection connection) {
        // 关闭结果集
        // ctrl+alt+m 将java语句抽取成方法
        closeResultSet(resultSet);//通过一个sql代理类去查询我们的sql语句，查出来的结果会封装一个resultset
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);//表示一个预编译SQL语句对象，并存储在PreparedStatement对象
        // 关闭连接 这里的关闭其实是将连接归还连接池
        closeConnection(connection);
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closePrepareStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
