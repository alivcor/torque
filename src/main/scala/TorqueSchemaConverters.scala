/*
 * Torque by Iresium - Created by alivcor (Abhinandan Dubey)
 *
 * Licensed under the Mozilla Public License Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://github.com/alivcor/torque/blob/main/LICENSE
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.iresium.torque

import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.avro.SchemaConverters.SchemaType
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.commons.io.IOUtils

import scala.collection.mutable._
import scala.collection.immutable._
import scala.collections.JavaConversions._
import java.io.{ByteArrayOutputStream, File}

import org.apache.avro.reflect.AvroSchema

import collection.JavaConverters._


class TorqueSchemaConverters {

    val ENFORCE_STRICT_CHECKING = false // This involves stricter field checking for BigDecimals, set to false for faster serialisation

    def matchFieldName(seqFieldsMap: ListBuffer[(String, String)], fieldName: String) = {
        seqFieldsMap.find(_._1 == fieldName) match {
            case Some(value) => true
            case None =>  false
        }
    }

    def fetchFieldInfo(seqFieldsMap: ListBuffer[(String, String)], fieldName: String) = {
        seqFieldsMap.find(_._1 == fieldName) match {
            case Some(value) => value
            case None =>  ("", "")
        }
    }

    def generateAvroSchema(sparkDF: DataFrame,
                           PROP_HANDLE_BIGINTS: Boolean = true) = {


        val spark = SparkSession.builder().master("local").getOrCreate()

        val structType: StructType = sparkDF.schema

        val recordName: String = "dfName"

        val builder: RecordBuilder[Schema] = SchemaBuilder.record(recordName).namespace(recordNamespace)
        val schema: Schema = SchemaConverters.convertStructToAvro(structType, builder, recordNamespace)

        var suspectedbigDecimalFieldsBuffer = new ListBuffer[(String, String)]()
        var finalizedbigDecimalFieldsBuffer = new ListBuffer[(String, String)]()

        // #TODO

    }
