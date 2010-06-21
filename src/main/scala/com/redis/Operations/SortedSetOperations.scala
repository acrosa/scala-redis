package com.redis.operations

import com.redis.Connection

/**
 * Redis sort set operations
 *
 */
trait SortedSetOperations{
  
  def getConnection(key: String): Connection
  
  // ZADD
  // Add the specified member having the specified score to the sorted set stored at key.
  def zSetAdd(key: String, score: Int, member: String): Boolean = {
    val connection = getConnection(key)
    connection.write("ZADD "+key+" "+score+" "+member.length+"\r\n"+member+"\r\n")
    connection.readBoolean
  }
  
  // ZREM
  // Remove the specified member from the sorted set value stored at key.
  def zSetDelete(key: String, member: String): Boolean = {
    val connection = getConnection(key)
    connection.write("ZREM "+key+" "+member.length+"\r\n"+member+"\r\n")
    connection.readBoolean
  }
  
  // ZINCRBY
  // If member already exists in the sorted set adds the increment to its score and updates the position of the element in the sorted set accordingly.
  def zSetIncrementBy(key: String, increment: Int, member: String): Option[Int] = {
    val connection = getConnection(key)
    connection.write("ZINCRBY "+key+" "+increment+" "+member.length+"\r\n"+member+"\r\n")
    connection.readString match {
      case Some(s: String) => Some(s.toInt)
      case _ => None
    }
  }
  
  // ZRANGE
  // Return the specified elements of the sorted set at the specified key.
  def zSetRange(key: String, start: Int, end: Int): Option[Set[String]] = {
    val connection = getConnection(key)
    connection.write("ZRANGE "+key+" "+start+" "+end+"\r\n")
    connection.readSet
  }
  
  // ZREVRANGE
  // Return the specified elements of the sorted set at the specified key.
  def zSetReverseRange(key: String, start: Int, end: Int): Option[Set[String]] = {
    val connection = getConnection(key)
    connection.write("ZREVRANGE "+key+" "+start+" "+end+"\r\n")
    connection.readSet
  }
  
  // ZRANGEBYSCORE
  // Return the all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
  def zSetRangeByScore(key: String, min: Int, max: Int): Option[Set[String]] = {
    val connection = getConnection(key)
    connection.write("ZRANGEBYSCORE "+key+" "+min+" "+max+"\r\n")
    connection.readSet
  }
  // with LIMIT offset count
  // 
  def zSetRangeByScore(key: String, min: Int, max: Int, offset: Int, count: Int): Option[Set[String]] = {
    val connection = getConnection(key)
    connection.write("ZRANGEBYSCORE "+key+" "+min+" "+max+" LIMIT "+offset+" "+count+"\r\n")
    connection.readSet
  }
  
  // ZCARD
  // Return the sorted set cardinality (number of elements).
  def zSetCount(key: String): Option[Int] = {
    val connection = getConnection(key)
    connection.write("ZCARD "+key+"\r\n")
    connection.readInt
  }
  
  // ZSCORE
  // Return the score of the specified element of the sorted set at key.
  def zSetScore(key: String, member: String): Option[Int] = {
    val connection = getConnection(key)
    connection.write("ZSCORE "+key+" "+member.length+"\r\n"+member+"\r\n")
    connection.readString match {
      case Some(s: String) => Some(s.toInt)
      case _ => None
    }
  }
}
