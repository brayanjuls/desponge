import java.sql.Timestamp

object Model {
  case class CategorizedContent(id:String,url:String,categories:Array[String],authors:Array[String],updated_at:Timestamp)

  case class Login(username: String, password: String)
  case class PostgresConf(
                           host: String,
                           port: Int,
                           dbName: String,
                           authMethods: Login,
                           url:String
                         )
}
