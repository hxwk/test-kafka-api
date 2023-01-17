package com.itheima.test

object Test {
  def main(args: Array[String]): Unit = {
    val str ="a#CS##CS##CS#"
    val arr1: Array[String] = str.split("#CS#")
    val arr2: Array[String] = str.split("#CS#",-1)

    print(s"arr1.length:${arr1.length},arr2.length:${arr2.length}")
  }
}
