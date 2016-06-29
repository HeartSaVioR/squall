/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
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

package ch.epfl.data.squall.api.scala.queries

import ch.epfl.data.squall.api.scala.SquallType._
import ch.epfl.data.squall.api.scala.Stream._
import ch.epfl.data.squall.query_plans.QueryBuilder
import ch.epfl.data.squall.query_plans.QueryPlan
import ch.epfl.data.squall.api.scala._
import ch.epfl.data.squall.api.scala.TPCHSchema._
import ch.epfl.data.squall.utilities.SquallContext
import com.esotericsoftware.kryo.Kryo

/**
 * @author mohamed
 * Hyracks Query
 *
 * SELECT C_MKTSEGMENT, COUNT(O_ORDERKEY)
 * FROM CUSTOMER join ORDERS on C_CUSTKEY = O_CUSTKEY
 * GROUP BY C_MKTSEGMENT
 */
class ScalaHyracksPlan(dataPath: String, extension: String, context: SquallContext) extends QueryPlan {

  override def getQueryPlan(): QueryBuilder = {
    val customers = Source[Customer]("customer").map { t => (t.custkey, t.mktsegment) }
    val orders = Source[Orders]("orders").map { _.custkey }
    val join = (customers join orders)(_._1)(x => x)
    val agg = join.groupByKey(x => 1, _._1._2)
    agg.execute(context)
  }
  

}

object main{
  
  def main(args: Array[String]): Unit = {
    
    val k = new Kryo()
    
  k.setRegistrationRequired(true);
      k.register(scala.collection.convert.Wrappers.SeqWrapper.getClass)
      
      val context = new SquallContext();
      context.setLocal()
      
      val plan = new ScalaHyracksPlan("/Users/elhachim/squall/test/data/tpch/0.01G", ".tbl", context).getQueryPlan()
      val result = context.submitLocalAndWait("scalahyracks", plan)
  }
  
}
