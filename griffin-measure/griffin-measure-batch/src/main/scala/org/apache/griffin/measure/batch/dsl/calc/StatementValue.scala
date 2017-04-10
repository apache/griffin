package org.apache.griffin.measure.batch.dsl.calc


trait StatementValue extends CalcValue {

}

case class AssignValue(right: ElementValue) extends StatementValue {

  val value: Option[Any] = None

}

case class ConditionValue(compareOpr: String, left: ElementValue, right: ElementValue, annotations: Iterable[AnnotationValue]) extends StatementValue {

  val value: Option[Boolean] = Some(true) // fixme: need calculation

}

case class MappingValue(mappingOpr: String, left: ElementValue, right: ElementValue, annotations: Iterable[AnnotationValue]) extends StatementValue {

  val value: Option[Boolean] = {
    (left.value, right.value) match {
      case (Some(v1), Some(v2)) => Some(v1 == v2)
      case (None, None) => Some(true)
      case _ => Some(false)
    }
  }

  def isKey: Boolean = {
    annotations.exists { e =>
      e match {
        case a: AnnotationValue => a.isKey
        case _ => false
      }
    }
  }
  def getName: String = {
    val names = annotations.flatMap { e =>
      e match {
        case a: AnnotationValue => a.getName
        case _ => None
      }
    }
    if (names.size > 0) names.head else {
      "???"   // fixme: need to try to get a name from left expr
    }
  }

}

case class StatementsValue(statements: Iterable[StatementValue]) extends StatementValue {

  val value: Option[Any] = None

}