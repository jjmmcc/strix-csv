package io.strixvaria

import scala.util.Failure

package object csv {

}

package csv {
  class ParseException(lineNum: Int, msg: String, cause: Throwable = null) 
    extends Exception(s"$lineNum: $msg", cause)
  {
    def this(lineNum: Int, cause: Throwable) = this(lineNum, "", cause)
  }
}