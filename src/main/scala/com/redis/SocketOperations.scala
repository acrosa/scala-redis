package com.redis

/**
 * Socket operations
 *
 */

import java.io._
import java.net.Socket

trait SocketOperations {
  
  // Response codes from the Redis server
  // they tell you what's coming next from the server.
  val ERR    = "-"
  val OK     = "+OK"
  val SINGLE = "+"
  val BULK   = "$"
  val MULTI  = "*"
  val INT    = ":"
  
  val host: String
  val port: Int
  
  // File descriptors.
  var socket: Socket = null
  var out: OutputStream = null
  var in: BufferedReader = null
  
  def getOutputStream: OutputStream = out
  def getInputStream: BufferedReader = in
  def getSocket: Socket = socket

  def connected = { getSocket != null }
  def reconnect = { disconnect && connect }
  
  // Connects the socket, and sets the input and output streams.
  def connect: Boolean = {
    try {
      socket = new Socket(host, port)
      out = getSocket.getOutputStream
      in = new BufferedReader(new InputStreamReader(getSocket.getInputStream));
      true
    } catch {
      case _ => clearFd; false;
    }
  }
  
  // Disconnects the socket.
  def disconnect: Boolean = {
    try {
      socket.close
      out.close
      in.close
      clearFd
      true
    } catch {
      case _ => false
    }
  }
  
  def clearFd = {
    socket = null
    out = null
    in = null
  }
  
  // Reads the server responses as Scala types.
  def readString: Option[String] = {
    readResponse match {
      case Some(s: String) => Some(s)
      case _ => None
    }
  }
  def readInt: Option[Int] = {
    readResponse match {
      case Some(i: Int) => Some(i)
      case _ => None
    }
  }
  def readBoolean: Boolean = {
    readResponse match {
      case Some(1) => true
      case Some(OK) => true
      case _  => false
    }
  }
  def readList: Option[List[Option[String]]] = {
    readResponse match {
      case Some(s: String) => listReply(s)
      case _ => None
    }
  }
  def readSet: Option[Set[String]] = {
    readResponse match {
      case Some(s: String) => setReply(s)
      case _ => None
    }
  }
  
  // Read from Input Stream.
  def readline: String = {
    try {
      getInputStream.readLine()
    } catch {
      case _ => ERR;
    }
  }
  
  // Gets the type of response the server is going to send.
  def readtype = {
    try {
      val res = readline
      if(res !=null){
        (res(0).toString(), res)
      }else{
        (ERR, "")
      }
    } catch {
      case e: Exception => (ERR, "")
    }
  }
  
  // Reads the response from the server based on the response code.
  def readResponse: Option[Any] = {
    val responseType = readtype
    try {
      responseType._1 match {
       case ERR     => reconnect; None; // RECONNECT
       case SINGLE  => lineReply(responseType._2)
       case BULK    => bulkReply(responseType._2)
       case MULTI   => Some(responseType._2)
       case INT     => integerReply(responseType._2)
       case _       => reconnect; None; // RECONNECT
      }
    } catch {
      case e: Exception => None
    }
  }

  def integerReply(response: String): Option[Int] = {
    response.split(":")(1).toString match {
      case "-1" => None
      case s: String => Some(Integer.parseInt(s))
    }
  }
  
  def lineReply(response: String): Option[String] = Some(response)
  
  def bulkReply(response: String): Option[String] = {
    if(response(1).toString() != ERR){
      var length: Int = Integer.parseInt(response.split('$')(1).split("\r\n")(0))
      var line, res: String = ""
      while(length >= 0){
        line = readline
        length -= (line.length+2)
        res += line
        if(length > 0) res += "\r\n"
      }
      Some(res)
    } else {
      None
    }
  }

  def listReply(response: String): Option[List[Option[String]]] = {
    val total = Integer.parseInt(response.split('*')(1))
    if(total != -1) {
      var list: List[Option[String]] = List()
      (1 to total).foreach { i =>
        list = list ::: List(bulkReply(readtype._2))
      }
      Some(list)
    } else {
      None
    }
  }

  def setReply(response: String): Option[Set[String]] = {
    val total = Integer.parseInt(response.split('*')(1))
    if(total != -1) {
      var set: Set[String] = Set()
      for(i <- 1 to total){
        bulkReply(readtype._2) match {
          case Some(x) => set += x
          case _ => return None
        }
      }
      Some(set)
    } else {
      None
    }
  }
  
  // Wraper for the socket write operation.
  def write_to_socket(data: String)(op: OutputStream => Unit) = op(getOutputStream)
  
  // Writes data to a socket using the specified block.
  def write(data: String) = {
    if(!connected) connect;
    write_to_socket(data){
      getSocket =>
        try {
          getSocket.write(data.getBytes)
        } catch {
          case _ => reconnect;
        }
    }
  }
  
  def writeMultiBulk(size: Int, command: String, arguments: Map[String, String]) = {
    write("*"+ (size+1) +"\r\n"+ bulkFormat(command) + mapToMultiBulkFormat(arguments))
  }

  def writeMultiBulk(size: Int, command: String, arguments: Seq[String]) = {
    write("*"+ (size+1) +"\r\n"+ bulkFormat(command) + arguments.map(bulkFormat(_)).mkString)
  }
  
  def bulkFormat(value: String): String = "$"+ value.length+"\r\n"+ value +"\r\n"
  
  def mapToMultiBulkFormat(m: Map[String, String]): String = m.map(x => bulkFormat(x._1) + bulkFormat(x._2)).mkString
}
