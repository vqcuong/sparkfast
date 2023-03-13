package com.sparkfast.core.util

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

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

  def prettyCaseClass[T <: AnyRef](t: T)(implicit tt: TypeTag[T], ct: ClassTag[T]): String = {
    val className = ct.runtimeClass.getSimpleName
    val members = tt.tpe.members.collect {
      case m if m.isMethod && m.asMethod.isCaseAccessor => m.asMethod
    }
    val valueString = members.map { member =>
      val memberValue = tt.mirror.reflect(t).reflectMethod(member)()
      s"${member.name} = $memberValue"
    }.mkString(", ")
    s"$className(" + valueString + ")"
  }
}
