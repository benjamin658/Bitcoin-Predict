package ExecuteLayer

/**
  * Created by benjamin658 on 2015/12/27.
  */
trait Executor {
  def get(query: String): String

  def create(query: String): String

  def update(query: String): String

  def delete(query: String): String

  def jsonStringify(anyRef: AnyRef): String
}
