// /*
//  * Copyright (2021) The Delta Lake Project Authors.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// package io.delta.sharing.server

// import java.util.concurrent.TimeUnit

// import com.google.common.cache.CacheBuilder
// import io.delta.standalone.internal.DeltaSharedTable

// import io.delta.sharing.kernel.internal.DeltaSharedTableKernel
// import io.delta.sharing.server.config.{ServerConfig, TableConfig}


// /**
//  * A class to load Delta tables from `TableConfig`. It also caches the loaded tables internally
//  * to speed up the loading.
//  */
// class DeltaSharedTableLoader(serverConfig: ServerConfig) {
//   private val deltaSharedTableCache = {
//     CacheBuilder.newBuilder()
//       .expireAfterAccess(60, TimeUnit.MINUTES)
//       .maximumSize(serverConfig.deltaTableCacheSize)
//       .build[String, DeltaSharedTable]()
//   }

//   def loadTable(tableConfig: TableConfig, useKernel: Boolean = false): DeltaSharedTableProtocol = {
//     if (useKernel) {
//       return new DeltaSharedTableKernel(
//         tableConfig,
//         serverConfig.preSignedUrlTimeoutSeconds,
//         serverConfig.evaluatePredicateHints,
//         serverConfig.evaluateJsonPredicateHints,
//         serverConfig.evaluateJsonPredicateHintsV2,
//         serverConfig.queryTablePageSizeLimit,
//         serverConfig.queryTablePageTokenTtlMs,
//         serverConfig.refreshTokenTtlMs
//       )
//     }
//     try {
//       val deltaSharedTable =
//         deltaSharedTableCache.get(
//           tableConfig.location,
//           () => {
//             new DeltaSharedTable(
//               tableConfig,
//               serverConfig.preSignedUrlTimeoutSeconds,
//               serverConfig.evaluatePredicateHints,
//               serverConfig.evaluateJsonPredicateHints,
//               serverConfig.evaluateJsonPredicateHintsV2,
//               serverConfig.queryTablePageSizeLimit,
//               serverConfig.queryTablePageTokenTtlMs,
//               serverConfig.refreshTokenTtlMs
//             )
//           }
//         )
//       if (!serverConfig.stalenessAcceptable) {
//         deltaSharedTable.update()
//       }
//       deltaSharedTable
//     } catch {
//       case CausedBy(e: DeltaSharingUnsupportedOperationException) => throw e
//       case e: Throwable => throw e
//     }
//   }
// }

package io.delta.sharing.server

import java.util.concurrent.TimeUnit
import com.google.common.cache.CacheBuilder
import io.delta.standalone.internal.DeltaSharedTable
import io.delta.sharing.kernel.internal.DeltaSharedTableKernel
import io.delta.sharing.server.config.{ServerConfig, TableConfig}
import java.sql.{Connection, DriverManager}

class DeltaSharedTableLoader(serverConfig: ServerConfig) {
  private val deltaSharedTableCache = {
    CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .maximumSize(serverConfig.deltaTableCacheSize)
      .build[String, DeltaSharedTable]()
  }

  // Method to fetch the table location from PostgreSQL
  private def fetchTableLocationFromPostgres(): Option[String] = {
    val url = "jdbc:postgresql://dep-database-us-east-1.c8xgxyogjacf.us-east-1.rds.amazonaws.com:5432/dep_postgres_dev" +
    "?user=postgres&password=SuOYMwYRBYNPzK1y1fqn&ssl=false"
    var tableLocation: Option[String] = None

    try {
      connection = DriverManager.getConnection(url)
      val statement = connection.createStatement()
      val query = "SELECT path FROM public.dep_metadata_marketplace WHERE key = 'path'"
      val resultSet = statement.executeQuery(query)

      if (resultSet.next()) {
        tableLocation = Some(resultSet.getString("path"))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
    tableLocation
  }

  // Main method to load the Delta table
  def loadTable(tableConfig: TableConfig, useKernel: Boolean = false): DeltaSharedTableProtocol = {
    if (useKernel) {
      return new DeltaSharedTableKernel(
        tableConfig,
        serverConfig.preSignedUrlTimeoutSeconds,
        serverConfig.evaluatePredicateHints,
        serverConfig.evaluateJsonPredicateHints,
        serverConfig.evaluateJsonPredicateHintsV2,
        serverConfig.queryTablePageSizeLimit,
        serverConfig.queryTablePageTokenTtlMs,
        serverConfig.refreshTokenTtlMs
      )
    }
    try {
      // Fetch the table location from PostgreSQL
      val tableLocation = fetchTableLocationFromPostgres().getOrElse {
        throw new Exception("Table location not found in PostgreSQL")
      }

      // Use the table location fetched from PostgreSQL
      val deltaSharedTable =
        deltaSharedTableCache.get(
          tableLocation,
          () => {
            new DeltaSharedTable(
              tableConfig.copy(location = tableLocation),  // Replace the location dynamically
              serverConfig.preSignedUrlTimeoutSeconds,
              serverConfig.evaluatePredicateHints,
              serverConfig.evaluateJsonPredicateHints,
              serverConfig.evaluateJsonPredicateHintsV2,
              serverConfig.queryTablePageSizeLimit,
              serverConfig.queryTablePageTokenTtlMs,
              serverConfig.refreshTokenTtlMs
            )
          }
        )
      if (!serverConfig.stalenessAcceptable) {
        deltaSharedTable.update()
      }
      deltaSharedTable
    } catch {
      case CausedBy(e: DeltaSharingUnsupportedOperationException) => throw e
      case e: Throwable => throw e
    }
  }
}
