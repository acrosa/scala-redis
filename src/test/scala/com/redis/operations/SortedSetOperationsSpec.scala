import org.specs._
import com.redis._

import org.specs.mock.Mockito
import org.mockito.Mock._
import org.mockito.Mockito._
import org.mockito.Mockito.doNothing

object SortedSetOperationsSpec extends Specification with Mockito {

  "Redis Client Sorted Set Operations" should {
    var client: RedisTestClient = null
    var connection: Connection = null
    
    doBefore{
      connection = mock[Connection]
      client = new RedisTestClient(connection)
    }
    
    "add a member to a sorted set" in {
      connection.readBoolean returns true
      client.zSetAdd("set", 0, "value") must beTrue
      connection.write("ZADD set 0 5\r\nvalue\r\n") was called
    }
    
    "delete a member of a sorted set" in {
      connection.readBoolean returns true
      client.zSetDelete("set","value") must beTrue
      connection.write("ZREM set 5\r\nvalue\r\n") was called
    }
    
    "increment by score" in {
      connection.readString returns Some("3")
      client.zSetIncrementBy("set", 3, "value") mustEqual Some(3)
      connection.write("ZINCRBY set 3 5\r\nvalue\r\n") was called
    }
    
    "return a range" in {
      connection.readSet returns Some(Set("a", "b", "c"))
      client.zSetRange("set", 0, 10) mustEqual Some(Set("a", "b", "c"))
      connection.write("ZRANGE set 0 10\r\n") was called
    }
    
    "return a reversed range" in {
      connection.readSet returns Some(Set("c", "b", "a"))
      client.zSetReverseRange("set", 0, 10) mustEqual Some(Set("c", "b", "a"))
      connection.write("ZREVRANGE set 0 10\r\n") was called
    }
    
    "return a range by score" in {
      connection.readSet returns Some(Set("c", "b", "a"))
      client.zSetRangeByScore("set", 0, 10) mustEqual Some(Set("c", "b", "a"))
      connection.write("ZRANGEBYSCORE set 0 10\r\n") was called
    }
    
    "return a range by score with offset and limit" in {
      connection.readSet returns Some(Set("b", "a"))
      client.zSetRangeByScore("set", 0, 10, 1, 2) mustEqual Some(Set("b", "a"))
      connection.write("ZRANGEBYSCORE set 0 10 LIMIT 1 2\r\n") was called
    }
    
    "return the count" in {
      connection.readInt returns Some(2)
      client.zSetCount("set") mustEqual Some(2)
      connection.write("ZCARD set\r\n") was called
    }
    
    "return the score of an element" in {
      connection.readString returns Some("2")
      client.zSetScore("set", "element") mustEqual Some(2)
      connection.write("ZSCORE set 7\r\nelement\r\n") was called
    }
  }
}