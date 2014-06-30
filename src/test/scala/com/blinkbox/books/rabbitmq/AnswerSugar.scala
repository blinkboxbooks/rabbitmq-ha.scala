package com.blinkbox.books.rabbitmq

import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import scala.language.implicitConversions

/**
 * A convenient pair of wrappers that lets you pass in closures to Mockito's doAnswer/thenAnswer methods.
 * See http://henningpetersen.com/post/10/using-mockito-answers-with-scala-2-9 for details.
 */
trait AnswerSugar {

  implicit def toAnswer[T](f: () => T): Answer[T] = new Answer[T] {
    override def answer(invocation: InvocationOnMock): T = f()
  }

  implicit def toAnswerWithArguments[T](f: (InvocationOnMock) => T): Answer[T] = new Answer[T] {
    override def answer(invocation: InvocationOnMock): T = f(invocation)
  }

}
