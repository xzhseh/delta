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

package org.apache.spark.sql.delta.uniform

import java.util.UUID
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.icebergShaded.IcebergTransactionUtils
import org.apache.spark.sql.delta.{DeltaLog, DeltaUnsupportedOperationException, UniFormWithIcebergCompatV1SuiteBase, UniFormWithIcebergCompatV2SuiteBase, UniversalFormatMiscSuiteBase, UniversalFormatSuiteBase}
import org.apache.spark.sql.{DataFrame, Row}

class UniversalFormatSuite
    extends UniversalFormatMiscSuiteBase
    with WriteDeltaHMSReadIceberg {
  override def withTempTableAndDir(f: (String, String) => Unit): Unit = {
    val tableId = s"testTable${UUID.randomUUID()}".replace("-", "_")
    withTempDir { dir =>
      val tablePath = new Path(dir.toString, "table")

      withTable(tableId) {
        f(tableId, s"'$tablePath'")
      }
    }
  }

  override def executeSql(sqlStr: String): DataFrame = {
    write(sqlStr)
    // return a null reference here to fulfill
    // override requirement
    null
  }

  test("create new UniForm table without manually enabling IcebergCompat - fail") {
    allReaderWriterVersions.filter { case (_, w) => w != 6 }.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        val e = intercept[DeltaUnsupportedOperationException] {
          executeSql(s"""
                        |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
                        |  'delta.universalFormat.enabledFormats' = 'iceberg',
                        |  'delta.minReaderVersion' = $r,
                        |  'delta.minWriterVersion' = $w
                        |)""".stripMargin)
        }
        assert(e.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION")

        val e1 = intercept[DeltaUnsupportedOperationException] {
          executeSql(s"""
                        |CREATE TABLE $id USING DELTA LOCATION $loc TBLPROPERTIES (
                        |  'delta.universalFormat.enabledFormats' = 'iceberg',
                        |  'delta.minReaderVersion' = $r,
                        |  'delta.minWriterVersion' = $w
                        |) AS SELECT 1""".stripMargin)
        }
        assert(e1.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION")
      }
    }
  }

  test("enable UniForm on existing table but IcebergCompat isn't enabled - fail") {
    allReaderWriterVersions.filter { case (_, w) => w != 6 }.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        executeSql(s"""
                      |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
                      |  'delta.minReaderVersion' = $r,
                      |  'delta.minWriterVersion' = $w
                      |)""".stripMargin)

        val e = intercept[DeltaUnsupportedOperationException] {
          executeSql(s"ALTER TABLE $id SET TBLPROPERTIES " +
            s"('delta.universalFormat.enabledFormats' = 'iceberg')")
        }
        assert(e.getErrorClass === "DELTA_UNIVERSAL_FORMAT_VIOLATION")
      }
    }
  }
}

class UniFormWithIcebergCompatV1Suite
    extends UniFormWithIcebergCompatV1SuiteBase
    with WriteDeltaHMSReadIceberg {
  override protected val allReaderWriterVersions: Seq[(Int, Int)] = (1 to 3)
    .flatMap { r => (1 to 7).filter(_ != 6).map(w => (r, w)) }
    // can only be at minReaderVersion >= 3 if minWriterVersion is >= 7
    .filterNot { case (r, w) => w < 7 && r >= 3 }

  override def withTempTableAndDir(f: (String, String) => Unit): Unit = {
    val tableId = s"testTable${UUID.randomUUID()}".replace("-", "_")
    withTempDir { dir =>
      val tablePath = new Path(dir.toString, "table")

      withTable(tableId) {
        f(tableId, s"'$tablePath'")
      }
    }
  }

  override def executeSql(sqlStr: String): DataFrame = {
    write(sqlStr)
    // return a null reference here to fulfill
    // override requirement
    null
  }
}

class UniFormWithIcebergCompatV2Suite
  extends UniFormWithIcebergCompatV2SuiteBase
    with WriteDeltaHMSReadIceberg {
  override protected val allReaderWriterVersions: Seq[(Int, Int)] = (1 to 3)
    .flatMap { r => (1 to 7).filter(_ != 6).map(w => (r, w)) }
    // can only be at minReaderVersion >= 3 if minWriterVersion is >= 7
    .filterNot { case (r, w) => w < 7 && r >= 3 }

  override def withTempTableAndDir(f: (String, String) => Unit): Unit = {
    val tableId = s"testTable${UUID.randomUUID()}".replace("-", "_")
    withTempDir { dir =>
      val tablePath = new Path(dir.toString, "table")

      withTable(tableId) {
        f(tableId, s"'$tablePath'")
      }
    }
  }

  override def executeSql(sqlStr: String): DataFrame = {
    write(sqlStr)
    // return a null reference here to fulfill
    // override requirement
    null
  }
}
