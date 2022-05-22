object AnonymousFunctions {
  object demo {
    def sum(x: Int, y: Int): Int = {
      return x + y
    }

    def sqr(x: Int)= {
      x * x
    }
  }


    def math(x: Int, y: Int): Unit = {
      println(x+y)
    }

def main(args:Array[String]): Unit = {
  var anfunc=(x:Int,y:Int)=>(x+y)
  println(anfunc(100,200))
  math(20,30)
  println(demo.sqr(5))
  println(demo.sum(500,100))
}

}
