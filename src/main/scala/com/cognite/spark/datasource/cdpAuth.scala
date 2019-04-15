package com.cognite.spark.datasource
import com.softwaremill.sttp.RequestT

sealed trait Auth {
  def auth[U[_], T, S](r: RequestT[U, T, S]): RequestT[U, T, S]
}

object Auth {
  implicit class AuthSttpExtension[U[_], T, +S](val r: RequestT[U, T, S]) {
    def auth(auth: Auth): RequestT[U, T, S] =
      auth.auth(r)
  }
}

case class ApiKeyAuth(apiKey: String) extends Auth {
  def auth[U[_], T, S](r: RequestT[U, T, S]): RequestT[U, T, S] =
    r.header("api-key", apiKey)
}

case class BearerTokenAuth(bearerToken: String) extends Auth {
  def auth[U[_], T, S](r: RequestT[U, T, S]): RequestT[U, T, S] =
    r.header("Authorization", s"Bearer $bearerToken")
}
