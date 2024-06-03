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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

case class CreateUniformTableStatement(
    table: LogicalPlan,
    ifNotExists: Boolean,
    isReplace: Boolean,
    isCreate: Boolean,
    fileFormat: String,
    metadataPath: String) extends UnaryNode {

  override def child: LogicalPlan = table

  override def output: Seq[Attribute] = Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

case class RefreshUniformTableStatement(
    target: LogicalPlan,
    isForce: Boolean,
    metadataPath: Option[String]) extends UnaryNode {

  override def child: LogicalPlan = target

  override def output: Seq[Attribute] = Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(target = newChild)
}
