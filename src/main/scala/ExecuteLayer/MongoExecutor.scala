package ExecuteLayer

/**
  * Created by benjamin658 on 2015/12/27.
  */
object MongoExecutor extends Executor {
  def get(query: String): String = {
    query
  }

  def create(query: String): String = {
    query
  }

  def update(query: String): String = {
    query
  }

  def delete(query: String): String = {
    query
  }

  def jsonStringify(anyRef: AnyRef): String = {
    ""
  }
}
