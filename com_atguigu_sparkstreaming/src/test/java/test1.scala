/**
 * <p>
 *
 * @description $description
 *              </p>
 * @project_name
 * @author Hefei
 * @since 2021/7/5 19:41 
 */
object test1 {
  def main(args: Array[String]): Unit = {
//    val i1: Int = ((1 to 5)).foldRight(100)((sum, i) => sum - i)
//    println(i1)
//    def findMax(temperatures: List[Int]) = {
//      temperatures.foldLeft(Integer.MIN_VALUE) {
//        Math.max
//      }
//      temperatures.foldRight(Integer.MIN_VALUE) {
//        Math.max
//      }
//
//    }
//    val car = "car"

    def getPersonInfo(primaryKey: Int): (String, String, String) = {
      // 假定 primaryKey 是用来获取用户信息的主键
      // 这里响应体是固定的
      ("Venkat", "Subramaniam", "venkats@agiledeveloper.com")
    }

    // 这里元组的每个元素都有自己的名字，但是元组没有名字
    val (firstName, lastName, emailAddress) = getPersonInfo(1)
    println(s"First Name: $firstName")
    println(s"Last Name: $lastName")
    println(s"Email Address: $emailAddress")
    // 这里元组有自己的名字info，但是元组中的每个元素没有名字，调用时使用info_1的方式进行调用
    val info: (String, String, String) = getPersonInfo(1)
    println(s"First Name: ${info._1}")
    println(s"Last Name: ${info._2}")
    println(s"Email Address: ${info._3}")
  }

  def turn(direction: String): Unit = {
  }
}
