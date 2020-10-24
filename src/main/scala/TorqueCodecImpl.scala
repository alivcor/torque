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

class TorqueCodec {

    def _encodeGenericRecord(avroGenericRec: GenericRecord, avroSchema: Schema, encoding: String = "UTF-8") = {

        val writer = new GenericDatumWriter[GenericRecord](avroSchema)
        val baos = new ByteArrayOutputStream
        val jsonEncoder = EncoderFactory.get.jsonEncoder(avroSchema, baos)
        writer.write(avroGenericRec, jsonEncoder)

        jsonEncoder.flush()
        baos.toString(encoding)
    }


    def encode(avroGenericRecords: List[GenericRecord], avroSchema: Schema, encoding: String = "UTF-8"): Unit = {
        val encodedGenericRecordBuffer = new ListBuffer[String]()

        for(record <- avroGenericRecords) {
            encodedGenericRecordBuffer += _encodeGenericRecord(record, avroSchema, encoding)
        }

        (encodedGenericRecordBuffer.toList, avroSchema.toString())
    }


    def decode(schema: String, genRecStr: String) = {
        _decodeGenericRecord(schema, genRecStr)
    }

    def _decodeGenericRecord(schema: String, genRecStr: String) = {

        val avroSchema = Schema.parse(schema)
        val decoder = DecoderFactory.get.jsonDecoder(avroSchema, genRecStr)
        val reader = new GenericDatumReader[GenericRecord](avroSchema)

        (reader.read(null, decoder), SchemaConverters.toSqlType(avroSchema))
    }
}
