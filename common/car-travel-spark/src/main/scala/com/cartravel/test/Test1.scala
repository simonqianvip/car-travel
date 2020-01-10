package com.cartravel.test

import com.cartravel.hbase.hutils.RowkeyUtils
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}

object Test1 {
  def main(args: Array[String]): Unit = {
    val str = MD5Hash.getMD5AsHex(Bytes.toBytes("simonqian"))//.substring(0, 15)
    print(s"MD5=${str}，length=${str.length}")

    var str1 = "qwe123sdf2342345"

    val splits: Array[String] = str1.split("")

    splits.foreach(x=>{
      println(x)
    })

  }

}
