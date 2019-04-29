package com.atguigu

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Author lzc
  * Date 2019-04-29 15:34
  *
  * 从一个网络端口接收数据,并统计单词的个数
  *
  */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    /*
     
     */
    override def onStart(): Unit = {
        // 启动一个子线程专门负责接收数据
        new Thread() {
            override def run(): Unit = {
                receive()
            }
        }.start()
    }
    
    def receive(): Unit = {
        // 启动一个socket, 然后从socket读取数据, 然后存储的DSteam中
        val socket = new Socket(host, port)
        // 从socket中读取数据
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
        
        var line: String = reader.readLine()
        while(line != null){
            // 存储到DSteam
            store(line)
            line = reader.readLine()
        }
    
        reader.close()
        socket.close()
        // 重启
        restart("retying....")
    }
    
    /*
        当Receiver结束的时候回调
     */
    override def onStop(): Unit = {}
}
