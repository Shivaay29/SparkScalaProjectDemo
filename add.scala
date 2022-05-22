object add {
  def main(args:Array[String]): Unit ={
    println("returnvalue :" + addInt(30,40))
  }
  def addInt( a:Int, b:Int ) : Int = {
    var sum:Int = 0
    sum = a + b
    return sum
  }
}
