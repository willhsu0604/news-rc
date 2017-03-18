package idv.will.model

class NewsBean(var id: String, var title: String, var content: String)
  extends EsElement with EsSerializable {

  def this() {
    this(null, null, null)
  }

}
