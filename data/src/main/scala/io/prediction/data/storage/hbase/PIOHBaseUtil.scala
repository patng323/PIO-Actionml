/** Copyright 2015 TappingStone, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
  
package org.apache.hadoop.hbase.mapreduce

/* Pretends to be hbase.mapreduce package in order to expose its
 * Package-accessible only static function convertScanToString()
 */

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64;

object PIOHBaseUtil {
  def convertScanToString(scan: Scan): String = {
    /*TableMapReduceUtil.convertScanToString(scan)*/
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray())
  }
}
