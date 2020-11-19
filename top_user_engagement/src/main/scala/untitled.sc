val jsonstring = """quote_count":2684}}],"meta":{"newest_id":"1328684558166986755","oldest_id":"1326993403268259840","result_count":3}}"""

val searchPattern = ".*oldest_id\":\"(\\d+).*".r
var searchUntil: String = null

jsonstring match {
  case searchPattern(id) => {searchUntil = id}
}
println(searchUntil)

val destName = "BarackObama"
println(s"""{"username":"$destName",""")