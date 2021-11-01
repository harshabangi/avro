import org.apache.avro.Schema

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

object Utility {

  def handleUnionType(b1: mutable.Buffer[Schema], b2: mutable.Buffer[Schema]): Boolean = {
    if (b1(0).getType == Schema.Type.NULL && b2(0).getType == Schema.Type.NULL) {
      equalsAvroSchema(b1(1), b2(1))
    } else if (b1(0).getType == Schema.Type.NULL && b2(1).getType == Schema.Type.NULL) {
      equalsAvroSchema(b1(1), b2(0))
    } else if (b1(1).getType == Schema.Type.NULL && b2(0).getType == Schema.Type.NULL) {
      equalsAvroSchema(b1(0), b2(1))
    } else {
      equalsAvroSchema(b1(0), b2(0))
    }
  }

  def equalsAvroSchema(s1: Schema, s2: Schema): Boolean = {
    (s1.getType, s2.getType) match {
      case (Schema.Type.ARRAY, Schema.Type.ARRAY) => equalsAvroSchema(s1.getElementType, s2.getElementType)
      case (Schema.Type.MAP, Schema.Type.MAP) => equalsAvroSchema(s1.getValueType, s2.getValueType)
      case (Schema.Type.UNION, Schema.Type.UNION) =>
        handleUnionType(s1.getTypes.asScala, s2.getTypes.asScala)
      case (Schema.Type.RECORD, Schema.Type.RECORD) =>
        s1.getFields.asScala.zip(s2.getFields.asScala).forall {
          case (l, r) => l.name().equals(r.name()) && equalsAvroSchema(l.schema(), r.schema())
        }
      case (from, to) => from.equals(to)
    }
  }
}
