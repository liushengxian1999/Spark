package Sort

object Sort3_rule {

  implicit object MySort3Rule extends Ordering[MySort3]{
    override def compare(x: MySort3, y: MySort3): Int = {

      if(y.money != x.money){

        y.money - x.money
      }else{
        x.age - y.age
      }

    }
  }
}
