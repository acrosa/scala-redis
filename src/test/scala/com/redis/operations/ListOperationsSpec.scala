import org.specs._
import com.redis._

import org.specs.mock.Mockito
import org.mockito.Mock._
import org.mockito.Mockito._
import org.mockito.Mockito.doNothing

object ListOperationsSpec extends Specification with Mockito {

  "Redis Client List Operations" should {
    var client: RedisTestClient = null
    var connection: Connection = null
    
    doBefore{
      connection = mock[Connection]
      client = new RedisTestClient(connection)
    }
    
    "push to head" in {
      connection.readBoolean returns true
      client.pushHead("k", "v") must beTrue
      connection.write("LPUSH k 1\r\nv\r\n") was called
    }
    
    "push to tail" in {
      connection.readBoolean returns true
      client.pushTail("k", "v") must beTrue
      connection.write("RPUSH k 1\r\nv\r\n") was called
    }
    
    "pop from head" in {
      connection.readString returns Some("value")
      client.popHead("key") mustEqual Some("value")
      connection.write("LPOP key\r\n") was called
    }
    
    "pop from tail" in {
      connection.readString returns Some("value")
      client.popTail("key") mustEqual Some("value")
      connection.write("RPOP key\r\n") was called
    }
    
    "return list index" in {
      connection.readString returns Some("value")
      client.listIndex("k", 2) mustEqual Some("value")
      connection.write("LINDEX k 2\r\n") was called
    }
    
    "return set element at index" in {
      connection.readBoolean returns true
      client.listSet("k", 1, "value") mustEqual true
      connection.write("LSET k 1 5\r\nvalue\r\n") was called
    }
    
    "return list size" in {
      connection.readInt returns Some(3)
      client.listLength("k") mustEqual Some(3)
      connection.write("LLEN k\r\n") was called
    }
    
    "return list range" in {
      val listResult = Some(List("one", "two", "three", "four", "five").map(Some(_)))
      connection.readList returns listResult
      client.listRange("k", 2, 4) mustEqual listResult
      connection.write("LRANGE k 2 4\r\n") was called
    }
    
    "trim a list" in {
      connection.readBoolean returns true
      client.listTrim("k", 2, 4) mustEqual true
      connection.write("LTRIM k 2 4\r\n") was called
    }
    
    "remove occurrences of a value in the list" in {
      connection.readBoolean returns true
      client.listRem("k", 2, "value") mustEqual true
      connection.write("LREM k 2 5\r\nvalue\r\n") was called
    }
    
    "Return and remove (atomically) the last element of the source List stored at _srckey_ and push the same element to the destination List stored at _dstkey_" in {
      connection.readString returns Some("valuefroma")
      client.rPopLPush("a", "b") mustEqual Some("valuefroma")
      connection.write("RPOPLPUSH a b\r\n") was called
    }
  }
}
