package com.atguigu.qzpoint.util

import java.sql.{Connection, PreparedStatement, ResultSet}

trait QueryCallback {
  def process(rs: ResultSet)
}

// sql代理类 实现动态参数传入SQL语句中
class SqlProxy {
  // 结果集
  private var resultSet: ResultSet = _
  // PreparedStatement表示预编译SQL语句的对象。 SQL语句被预编译并存储在PreparedStatement对象中。 然后可以使用此对象多次有效地执行此语句。
  private var preparedStatement: PreparedStatement = _

  /**
   * 执行更新/修改语句
   * 不能用select * ，因为它返回值是一个结果集，
   * 返回值是int类型接收的
   *
   * @param conn 数据库连接对象
   * @param sql 需要执行的SQL语句
   * @param params 占位符
   * @return (1) SQL 数据操作语言 (DML) 语句的行数或 (2) 0 —— 对于不返回任何内容的 SQL 语句
   */
  def executeUpdate(conn: Connection, sql: String, parametersArray: Array[Any]): Int = {
    var result = 0
    try {
      //预执行
      preparedStatement = conn.prepareStatement(sql)
      // 进行非空校验
      if (parametersArray != null && parametersArray.length > 0) {
        for (i <- 0 until parametersArray.length) {
          // setObject方法的第一个参数parameterIndex是从1开始的
          preparedStatement.setObject(i + 1, parametersArray(i))
        }
      }
      //注意！这里不是递归 此executeUpdate非我们当前的executeUpdate方法！
      // executeUpdate方法的返回值：(1) SQL 数据操作语言 (DML) 语句的行数或 (2) 0 对于不返回任何内容的 SQL 语句
      result = preparedStatement.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  /**
   * 执行查询语句
   * @param conn 连接
   * @param sql 需要执行的SQL语句
   * @param params 占位符
   * @param queryCallback 回调函数 将结果集rs返回进行具体业务的操作
   */
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback) = {
    resultSet = null
    try {
      preparedStatement = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          preparedStatement.setObject(i + 1, params(i))
        }
      }
      //注意！这里不是递归
      resultSet = preparedStatement.executeQuery()
      //调了特质里的抽象方法，所以在用这个方法的时候要写一个匿名实现类，这里需要传入我们处理该结果集的具体逻辑。
      queryCallback.process(resultSet)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def shutdown(conn: Connection): Unit = DataSourceUtil.closeResource(resultSet, preparedStatement, conn)
}
