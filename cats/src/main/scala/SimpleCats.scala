package com.eztier.examples

// From: https://developer.ibm.com/tutorials/cl-model-first-microservices-scala-cats/

case class Author(id: Long, name: String)

case class Publication(id: Long, authorId: Long, title:String)

case class AuthorPublications(author: Author, publications: List[Publication])

object SimpleCatsApp {
  
  sealed trait ServiceError
  case object InvalidQuery extends ServiceError
  case object NotFound extends ServiceError

  def renderResponse(statusCode: Int, response: String): Unit

  def findAuthor(query: String): FutureLong

  def getAuthor(id: Long): FutureAuthor

  def getPublications(authorId: Long): FutureList[Publication]

  def findPublications(query: String): Future[AuthorPublications] =
    for {
      authorId <- findAuthor(query)
      author <- getAuthor(authorId)
      pubs <- getPublications(authorId)
    } yield AuthorPublications(author, pubs)

  val query = "matthias k"

  /*
  // First version, just scala
  
  val search: Future[Unit] = findPublications(query) map { authorPubs =>
    renderResponse(200, s"Found $authorPubs")
  }
  
  */

  // Either
  // with extensions (asRight, asLeft) with:
  // import cats._, implicits._
  def findAuthor(query: String): Future[Either[ServiceError, Long]] =
    Future.successful(
      if (query == "matthias k") 42L.asRight
      else if (query.isEmpty) InvalidQuery.asLeft
      else NotFound.asLeft
    )

  def findPublications(query: String): Future[AuthorPublications] =
    for {
      authorId <- findAuthor(query)
      (author, pubs) <- getAuthor(authorId) product getPublications(authorId)
    } yield AuthorPublications(author, pubs)

  
  /*  
    EitherT[F[_], A, B] is a lightweight wrapper for F[Either[A, B]] that makes it easy to compose Eithers and Fs together.
    reference:
      https://typelevel.org/cats/datatypes/eithert.html
  */

  // bind Future and SearchError together while leaving the inner result type unbound
  type Result[A] = EitherT[Future, SearchError, A]
  
  // this allows you to invoke the companion object as "Result"
  val Result = EitherT
  
  def findAuthor(query: String): Result[Long] =
    if (query == "matthias k") Result.rightT(42L)
    else if (query.isEmpty) Result.leftT(InvalidQuery)
    else Result.leftT(NotFound)

  // Nested maps
  def getAuthor(id: Long): Result[Author] = 
    Result.rightT(Author(id, "Matthias Käppler"))

  def getPublications(authorId: Long): Result[List[Publication]] =
    Result.rightT(List(
      Publication(1L, authorId, "Model‑First Microservices with Scala & Cats")
    ))

  // EitherT is itself a monad that simply stacks Future and Either
  def findPublications(query: String): Result[AuthorPublications] =
    for {
      authorId <- findAuthor(query)
      result <- getAuthor(authorId) product getPublications(authorId)
      (author, pubs) = result
    } yield AuthorPublications(author, pubs)

  val search: Result[Unit] = findPublications(query) map { authorPubs =>
      renderResponse(200, s"Found $authorPubs")
    } recover {
      case InvalidQuery => renderResponse(400, s"Not a valid query: '$query'")
      case NotFound => renderResponse(404, s"No results found for '$query'")
    }


  def main(args: Array[String]) {

    // Standard scala.
    Await.result(search, Duration.Inf)

    // Semigroupal - joining Futures using product
    // Future[Author] multiply Future[List[Publication]] => Future[(Author, List[Publication])]
    
    // Final version with cats.
    Await.result(search.value, Duration.Inf)
  }
}
