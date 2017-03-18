package idv.will.model

class KeywordBean(var id: String, var newsId: String, var keyword: String, var score: Double)
  extends EsElement with EsSerializable {

  def this() {
    this(null, null, null, 0.0)
  }

}
