package util

import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder

object Twitter {
  def auth(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String): Option[Authorization] = {
    val c =
      new ConfigurationBuilder()
        .setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(consumerSecret)
        .setOAuthAccessToken(accessToken)
        .setOAuthAccessTokenSecret(accessTokenSecret)
        .build()
    Option(new OAuthAuthorization(c))
  }

}
