package com.sparkfast.core.util



import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, Type, runtimeMirror}

//noinspection ScalaWeakerAccess
object ReflectUtil {

  def getFieldValueByName[T <: AnyRef](t: T, fieldName: String): AnyRef = {
    val field = t.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(t)
  }

  def isInstanceOfCaseClass[T <: AnyRef](t: T)(implicit tt: TypeTag[T], ct: ClassTag[T]): Boolean = {
    val typeMirror = tt.mirror
    val instanceMirror = typeMirror.reflect(t)
    instanceMirror.symbol.isCaseClass
  }

  def prettyCaseClass[V](v: V, t: Type = null)(implicit tt: TypeTag[V], ct: ClassTag[V]): String = {
    val className = if (t != null) t.typeSymbol.name else tt.tpe.typeSymbol.name
    val tMirror = tt.mirror.reflect(v)
    val tpe = if (t != null) t else tt.tpe
    val valueString = tpe.members.filter(!_.isMethod).map { member =>
      val memberValue = tMirror.reflectField(member.asTerm).get
      if (member.typeSignature.typeSymbol.asClass.isCaseClass) {
        val prettyMemberValue = prettyCaseClass(
          memberValue,
          t = member.typeSignature.typeSymbol.asClass.selfType
        )
        Tuple2(member.name.toString.strip(), prettyMemberValue)
      } else {
        Tuple2(member.name.toString.strip(), memberValue)
      }
    }
      .toList.sortBy(_._1)
      .map { t => s"${t._1} = ${t._2}"}
      .mkString(", ")
    s"$className(" + valueString +")"
  }
}
