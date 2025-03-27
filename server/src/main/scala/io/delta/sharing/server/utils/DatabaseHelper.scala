// scalastyle:off

/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.server.utils

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.delta.sharing.server.SubscriptionExpiredException
import org.slf4j.LoggerFactory

import java.sql._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DatabaseHelper {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Azure SQL Server Connection String
  private val url =
    "jdbc:sqlserver://ceer-ods.database.windows.net:1433;" +
      "database=data-marketplace;user=dbadmin;password=Ceer@123456;" +
      "encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;" +
      "loginTimeout=30;"

  def checkTokenPresentInDb(token: String): Boolean = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Insert Query
      // val query = "select user_id from user_subscriptions where token = ?"

      val query = "SELECT user_id FROM user_subscriptions WHERE token = ? UNION SELECT user_id FROM user_group_subscriptions WHERE token = ?"

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, token)
      preparedStatement.setString(2, token)
      resultSet = preparedStatement.executeQuery();

      return resultSet.next()
    } catch {
      case e: Exception =>
        logger.error("Error checking token in database", e)
        false // Return false if there's an error
    } finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }

  def validateUserSubscriptionAndQueryLimit(userId: String, productCatalogId: String): Boolean = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Query
      val query = "SELECT expiration_date, subscription_pricing_detail, queries_used FROM user_subscriptions WHERE user_id = ? AND product_catalog_id = ?"

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, userId)
      preparedStatement.setString(2, productCatalogId)
      resultSet = preparedStatement.executeQuery()

      if (resultSet.next()) {
        // Extract values from result set
        val expirationDateTimeStr = resultSet.getString("expiration_date")
        val subscriptionPricingDetail = resultSet.getString("subscription_pricing_detail")
        val queriesUsed = resultSet.getInt("queries_used")
        logger.info("detail: {}", subscriptionPricingDetail);

        val objectMapper = new ObjectMapper();
        val jsonNode: JsonNode = objectMapper.readTree(subscriptionPricingDetail)
        val subscriptionType = jsonNode.get("type").asText();
        // Parse expiration_date as LocalDateTime
        if("subscription".equalsIgnoreCase(subscriptionType)){
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS") // Adjust if necessary
          val expirationDateTime = LocalDateTime.parse(expirationDateTimeStr, formatter)
          logger.info("expirationDateTime: {}", expirationDateTime);
          val currentDateTime = LocalDateTime.now()

          // Check if subscription has expired
          if (expirationDateTime.isBefore(currentDateTime)) {
            throw new SubscriptionExpiredException("Your subscription plan has expired at: "+expirationDateTime)
          }

          // Parse JSON to extract queryLimit
          val queryLimit = jsonNode.get("queryLimit").asInt()

          // Check if query limit is reached
          if (queriesUsed >= queryLimit) {
            throw new SubscriptionExpiredException("Your query limit has been reached to "+queryLimit)
          }
        }


        true // Valid subscription and within query limit
      } else {
        throw new Exception("Data not found")
      }
    }
    finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }

  def validateUserSubscriptionAndQueryLimitGroup(userId: String, groupName: String): Boolean = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Query
      val query = "select group_name,subscription_pricing_detail,expiration_date,  SUM(queries_used) as totalCount from user_group_subscriptions where group_name = ? group by group_name,subscription_pricing_detail,expiration_date;"

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, groupName)
      resultSet = preparedStatement.executeQuery()

      if (resultSet.next()) {
        // Extract values from result set
        val expirationDateTimeStr = resultSet.getString("expiration_date")
        val subscriptionPricingDetail = resultSet.getString("subscription_pricing_detail")
        val totalCount = resultSet.getInt("totalCount")
        logger.info("detail: {}", subscriptionPricingDetail);

        val objectMapper = new ObjectMapper();
        val jsonNode: JsonNode = objectMapper.readTree(subscriptionPricingDetail)
        val subscriptionType = jsonNode.get("type").asText();
        // Parse expiration_date as LocalDateTime
        if("subscription".equalsIgnoreCase(subscriptionType)){
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS") // Adjust if necessary
          val expirationDateTime = LocalDateTime.parse(expirationDateTimeStr, formatter)
          logger.info("expirationDateTime: {}", expirationDateTime);
          val currentDateTime = LocalDateTime.now()

          // Check if subscription has expired
          if (expirationDateTime.isBefore(currentDateTime)) {
            throw new SubscriptionExpiredException("Your subscription plan has expired at: "+expirationDateTime)
          }

          // Parse JSON to extract queryLimit
          val queryLimit = jsonNode.get("queryLimit").asInt()
          logger.info("**********************totalCount: ", totalCount);

          // Check if query limit is reached
          if (totalCount >= queryLimit) {
            throw new SubscriptionExpiredException("Your query limit has been reached to "+queryLimit)
          }
        }


        true // Valid subscription and within query limit
      } else {
        throw new Exception("Data not found")
      }
    }
    finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }

  def updateUserQueryAuditTable(userId: String, productCatalogId: String, productCatalogName: String, groupName: String): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Insert Query
      val query =
      if (groupName.nonEmpty) {
        "SELECT queries_used FROM user_group_subscriptions WHERE user_id = ? AND product_catalog_id = ?"
      } else {
        "SELECT queries_used FROM user_subscriptions WHERE user_id = ? AND product_catalog_id = ?"
      }

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, userId)
      preparedStatement.setString(2, productCatalogId)
      resultSet = preparedStatement.executeQuery()

      if (resultSet.next()) {
        // Extract values from result set
        val queriesUsed = resultSet.getInt("queries_used")
        val updatedQueriesUsed = queriesUsed + 1;

        // Prepare UPDATE statement
        val updateQuery =
        if (groupName.nonEmpty) {
          "UPDATE user_group_subscriptions SET queries_used = ? WHERE user_id = ? AND product_catalog_id = ?"
        } else {
          "UPDATE user_subscriptions SET queries_used = ? WHERE user_id = ? AND product_catalog_id = ?"
        }

        preparedStatement = connection.prepareStatement(updateQuery)
        preparedStatement.setInt(1, updatedQueriesUsed) // Incremented value
        preparedStatement.setString(2, userId)
        preparedStatement.setString(3, productCatalogId)

        // Execute UPDATE query
        val rowsUpdated = preparedStatement.executeUpdate()

        if (rowsUpdated == 0) {
          throw new Exception("Failed to update queries_used: No rows affected")
        }
        val query1 = "INSERT INTO user_query_audit (user_id, catalog_id, catalog_name, query_count, time_created,group_name) VALUES (?, ?, ?, ?, ?,?)"
        
        preparedStatement = connection.prepareStatement(query1)
        preparedStatement.setString(1, userId)
        preparedStatement.setString(2, productCatalogId)
        preparedStatement.setString(3, productCatalogName)
        preparedStatement.setInt(4, updatedQueriesUsed)
        preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()))
        preparedStatement.setString(6, groupName)
        preparedStatement.executeUpdate();
      }
    } catch {
      case e: SQLException =>
        logger.error("Database error in updateUserQueryAuditTable", e)
        throw new Exception("Database error: " + e.getMessage)
      case e: Exception =>
        logger.error("Error in updateUserQueryAuditTable", e)
        throw new Exception("Unexpected error: " + e.getMessage)
    } finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }

  def executeQuery(query: String): Seq[String] = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    var result = Seq[String]()

    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      resultSet = preparedStatement.executeQuery()

      while (resultSet.next()) {
        // Assuming the result has one column; modify accordingly if needed
        result = result :+ resultSet.getString(1)
      }
    } catch {
      case e: SQLException =>
        logger.error(s"Database error while executing query: $query", e)
        throw new Exception(s"Database error: ${e.getMessage}")
      case e: Exception =>
        logger.error(s"Error while executing query: $query", e)
        throw new Exception(s"Unexpected error: ${e.getMessage}")
    } finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }

    result
  }
}

