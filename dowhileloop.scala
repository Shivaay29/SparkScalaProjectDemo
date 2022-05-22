object dowhileloop {
  def main(args:Array[String]): Unit = {
    var name = "Ram prasad kumar singh"
    var a = 0
    do {
      println(name(a))
      a=a+1
    }
    while(a<name.length)
  }
}
