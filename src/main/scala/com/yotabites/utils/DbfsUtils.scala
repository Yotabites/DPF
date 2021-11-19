package com.yotabites.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.json.JSONObject

import scala.util.{Failure, Success, Try}

object DbfsUtils extends LazyLogging {

  final val ACCESS_KEY_SCOPE = "access.key.scope"
  final val ACCESS_KEY_TOKEN = "access.key.token"
  final val SECRET_ACCESS_KEY_SCOPE = "secret.access.key.scope"
  final val SECRET_ACCESS_KEY_TOKEN = "secret.access.key.token"
  final val s3MountPrefix = "/mnt"

  def setupMountPoint(location: String, s3CredentialAttributes: Config, custom: JSONObject): String = {
    val bucketName = getBucketName(location, s3CredentialAttributes, custom)
    //mount point name will be same as bucketName
    var mountPoint = {
      if (bucketName.contains("@")) bucketName.substring(bucketName.lastIndexOf("@") + 1) else bucketName
    }
    logger.info(s"Mount Point Name: $mountPoint")

    val status = Try {
      if (!isMountPointAvailable(mountPoint))
        dbutils.fs.mount(s"s3a://$bucketName", s"$s3MountPrefix/$mountPoint")
      else true
    }

    status match {
      case Success(b) => {
        if (b.asInstanceOf[Boolean]) logger.info(s"Mount Point $mountPoint successfully created.")
        else logger.error(s"Mount Point creation failed $mountPoint")
      }
      case Failure(f) => logger.error(AppUtils.getStackTrace(f))
        logger.error("Update to DeltaLake metastore failed...")
    }

    logger.info("Available mount points: " + dbutils.fs.ls("/mnt").map(info => info.name).mkString(", "))
    s3MountPrefix + location
  }

  private def getBucketName(location: String, s3CredentialAttributes: Config, custom: JSONObject): String = {
    // Expecting location to always start with- /<bucketName>/...  or  <bucketName>/...
    val bucketName = if (location.startsWith("/")) location.split("/")(1) else location.split("/")(0)

    //prefix bucketName with s3 credentials in the format: <accesskey>:<secret_accesskey>@<bucketName>
     if (!isMountPointAvailable(bucketName)) {
      if (custom != null
        && !custom.isNull(ACCESS_KEY_SCOPE) && !isBlank(Some(custom.getString(ACCESS_KEY_SCOPE)))
        && !custom.isNull(ACCESS_KEY_TOKEN) && !isBlank(Some(custom.getString(ACCESS_KEY_TOKEN)))
        && !custom.isNull(SECRET_ACCESS_KEY_SCOPE) && !isBlank(Some(custom.getString(SECRET_ACCESS_KEY_SCOPE)))
        && !custom.isNull(SECRET_ACCESS_KEY_TOKEN) && !isBlank(Some(custom.getString(SECRET_ACCESS_KEY_TOKEN))) ) {

          val akScope = custom.getString(ACCESS_KEY_SCOPE)
          val akKey = custom.getString(ACCESS_KEY_TOKEN)
          val skScope = custom.getString(SECRET_ACCESS_KEY_SCOPE)
          val skKey = custom.getString(SECRET_ACCESS_KEY_TOKEN)
          getCredentials(akScope,akKey,skScope,skKey) + "@" + bucketName

      } else if (s3CredentialAttributes != null
        && s3CredentialAttributes.hasPath(ACCESS_KEY_SCOPE) && !isBlank(Some(s3CredentialAttributes.getString(ACCESS_KEY_SCOPE)))
        && s3CredentialAttributes.hasPath(ACCESS_KEY_TOKEN) && !isBlank(Some(s3CredentialAttributes.getString(ACCESS_KEY_TOKEN)))
        && s3CredentialAttributes.hasPath(SECRET_ACCESS_KEY_SCOPE) && !isBlank(Some(s3CredentialAttributes.getString(SECRET_ACCESS_KEY_SCOPE)))
        && s3CredentialAttributes.hasPath(SECRET_ACCESS_KEY_TOKEN) && !isBlank(Some(s3CredentialAttributes.getString(SECRET_ACCESS_KEY_TOKEN))) ) {

          val akScope = s3CredentialAttributes.getString(ACCESS_KEY_SCOPE)
          val akKey = s3CredentialAttributes.getString(ACCESS_KEY_TOKEN)
          val skScope = s3CredentialAttributes.getString(SECRET_ACCESS_KEY_SCOPE)
          val skKey = s3CredentialAttributes.getString(SECRET_ACCESS_KEY_TOKEN)
          getCredentials(akScope,akKey,skScope,skKey) + "@" + bucketName

      } else bucketName
    } else bucketName
  }

  private def getCredentials(akScope: String, akKey: String,skScope: String,skKey: String) : String = {
    {
      //first try to get aws access key, secret access key from the Databricks secrets
      //if entry not found in Databricks secrets then, use akKey and skKey as access access key and secret access key
      var accessKey = getSecret(akScope, akKey)
      if (accessKey.isEmpty) accessKey = akKey
      var secretAccessKey = getSecret(skScope, skKey)
      if (secretAccessKey.isEmpty) secretAccessKey = skKey
      secretAccessKey = secretAccessKey.asInstanceOf[String].replaceAll("/", "%2F")
      accessKey + ":" + secretAccessKey
    }
  }

  def isMountPointAvailable(mountPoint: String) : Boolean = {
    val mountList = dbutils.fs.ls(s3MountPrefix)
    val mount = mountList.find(info => info.name.equals(mountPoint + "/"))
    mount match {
      case Some(m) => logger.info(m.name + " is already mounted.")
        true
      case None => false
    }
  }

  def unMount(mountPoint: String): Unit = {
    try {
      dbutils.fs.unmount(mountPoint)
    } catch {
      case t: Throwable =>
    }

  }

  def getSecret(scope: String, key: String) : String = Try {
    {
      try {
        dbutils.secrets.get(scope = scope, key = key)
      } catch {
        case t: Throwable => logger.error(AppUtils.getStackTrace(t))
          ""
      }
    }
  }.getOrElse("")


  def isBlank( input : Option[String]) : Boolean =
    input match {
      case None    => true
      case Some(s) => s.trim.isEmpty
    }

  def exit(msg: String): Unit = {
    dbutils.notebook.exit(msg)
  }

}
