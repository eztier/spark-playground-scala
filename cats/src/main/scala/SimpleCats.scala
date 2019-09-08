package com.eztier.examples

// ./Server.scala
object Server extends IOApp {
  def createServer[F[_]]: Resource[F, H4Server[F]] = ???
  def run(args : List[String]) : IO[ExitCode] = createServer.use(_ => IO.never).as(ExitCode.Success)
}

// ./Domain

// ./Domain/ValidationError.scala

sealed trait ValidationError extends Product with Serializable
case object PublicationNotFoundError extends ValidationError
case object AuthorNotFoundError extends ValidationError
case class AuthorAlreadyExistsError(author: Author) extends ValidationError

// ./Domain/authors

// ./Domain/authors/Author.scala
case class Author(
  firstName: String,
  lastName: String,
  email: String,
  phone: String,
  id: Option[Long] = None
)

// ./Domain/authors/AuthorRepositoryAlgebra.scala
import cats.data.OptionT

trait AuthorRepositoryAlgebra[F[_]] {
  def get(id: Long): OptionT[F, Author]

  def findByEmail(email: String): OptionT[F, Author]
}

// ./Domain/authors/AuthorValidationAlgebra.scala
import cats.data.EitherT

trait AuthorValidationAlgebra[F[_]] {
  /* Fails with a AuthorAlreadyExistsError */
  def doesNotExist(author: Author): EitherT[F, AuthorAlreadyExistsError, Unit]

  /* Fails with a AuthorNotFoundError if the authot id does not exist or if it is none */
  def exists(authorId: Option[Long]): EitherT[F, AuthorNotFoundError.type, Unit]
}

// ./Domain/authors/AuthorValidationInterpreter.scala
import cats.Applicative
import cats.data.EitherT
import cats.implicits._

class AuthorValidationInterpreter[F[_]: Applicative](authorRepo: AuthorRepositoryAlgebra[F]) extends AuthorValidationAlgebra[F] {
  def doesNotExist(author: Author): EitherT[F, AuthorAlreadyExistsError, Unit] =
    authorRepo
      .findByEmail(author.email)
      .map(AuthorAlreadyExistsError)
      .toLeft(())

  def exists(authorId: Option[Long]): EitherT[F, AuthorNotFoundError.type, Unit] =
    authorId match {
      case Some(id) =>
        authorRepo.get(id)
          .toRight(AuthorNotFoundError)
          .void
      case None =>
        // EitherT.leftT, EitherT.rightT is alias for EitherT.pure
        EitherT.left[Unit](AuthorNotFoundError.pure[F])
    }
}

object AuthorValidationInterpreter {
  def apply[F[_]: Applicative](repo: AuthorRepositoryAlgebra[F]): AuthorValidationAlgebra[F] =
    new AuthorValidationInterpreter[F](repo)
}

// ./Domain/authors/AuthorService.scala
import cats.data._
import cats.Functor
import cats.Monad
import cats.syntax.functor._

class AuthorService[F[_]](authorRepo: AuthorRepositoryAlgebra[F], validation: AuthorValidationAlgebra[F]) {
  def getAuthor(id: Long)(implicit F: Functor[F]): EitherT[F, AuthorNotFoundError.type, User] =
    authorRepo.get(id).toRight(AuthorNotFoundError)

  def getAuthorByEmail(email: String)(implicit F: Functor[F]): EitherT[F, AuthorNotFoundError.type, Author] =
    authorRepo.findByEmail(email).toRight(AuthorNotFoundError)
}

object AuthorService {
  def apply[F[_]](repository: AuthorRepositoryAlgebra[F], validation: AuthorValidationAlgebra[F]): AuthorService[F] =
    new AuthorService[F](repository, validation)
}


// ./Domain/publications

// ./Domain/publications/Publication.scala
import java.time.Instant

case class Publication(
  publicationDate: Option[Instant] = None,
  title: String,
  id: Option[Long] = None,
  authorId: Long
)

// ./Domain/publications/PublicationRepositoryAlgebra.scala
trait PublicationRepositoryAlgebra[F[_]] {
  def get(id: Long): F[Option[Publication]]

  def findByAuthorId(authorId: Long): F[List[Publication]]
}

// ./Domain/publications/PublicationValidationAlgebra.scala
import cats.data.EitherT

trait PublicationValidationAlgebra[F[_]] {
  /* Fails with a PublicationAlreadyExistsError */
  // def doesNotExist(publication: Publication): EitherT[F, PublicationAlreadyExistsError, Unit]

  /* Fails with a PetNotFoundError if the pet id does not exist or if it is none */
  def exists(id: Option[Long]): EitherT[F, PublicationNotFoundError.type, Unit]
}

// ./Domain/publications/PublicationValidationInterpreter.scala
import cats.Applicative
import cats.data.EitherT
import cats.implicits._

class PublicationValidationInterpreter[F[_]: Applicative](repository: PublicationRepositoryAlgebra[F]) extends PublicationValidationAlgebra[F] {

  def exists(id: Option[Long]): EitherT[F, PublicationNotFoundError.type, Unit] =
    EitherT {
      id match {
        case Some(id) =>
          repository.get(id).map {
            case Some(_) => Right(())
            case _ => Left(PublicationNotFoundError)
          }
        case _ =>
          Either.left[PublicationNotFoundError.type, Unit](PublicationNotFoundError).pure[F]
      }
    }
}

object PublicationValidationInterpreter {
  def apply[F[_]: Applicative](repository: PublicationRepositoryAlgebra[F]) =
    new PublicationValidationInterpreter[F](repository)
}

// ./Domain/publications/PublicationService.scala

import cats.Functor
import cats.data._
import cats.Monad
import cats.syntax.all._

/**
  * The entry point to our domain, works with repositories and validations to implement behavior
  * @param repository where we get our data
  * @param validation something that provides validations to the service
  * @tparam F - this is the container for the things we work with, could be scala.concurrent.Future, Option, anything
  *           as long as it is a Monad
  */
class PublicationService[F[_]](
  repository: PublicationRepositoryAlgebra[F],
  validation: PublicationValidationAlgebra[F]
) {

  def get(id: Long)(implicit F: Functor[F]): EitherT[F, PublicationNotFoundError.type, Publication] =
    EitherT.fromOptionF(repository.get(id), PublicationNotFoundError)

  def findByAuthorId(authorId: Long): F[List[Publication]] =
    repository.findByAuthorId(authorId)
}

object PublicationService {
  def apply[F[_]](repository: PublicationRepositoryAlgebra[F], validation: PublicationValidationAlgebra[F]): PublicationService[F] =
    new PublicationService[F](repository, validation)
}


// ./Infrastructure

// ./Infrastruture/repository/doobie

// ./Infrastruture/repository/doobie/DoobieAuthorRepositoryInterpreter.scala
import cats.data.OptionT
import cats.effect.Bracket
import cats.implicits._
import doobie._
import doobie.implicits._
import io.circe.parser.decode
import io.circe.syntax._

private object AuthorSQL {

  def findOne(id: Long): Query0[Author] = sql"""
    SELECT FIRST_NAME, LAST_NAME, EMAIL, PHONE, ID
    FROM AUTHORS
    WHERE ID = $id
  """.query

  def find(email: String): Query0[Author] = sql"""
    SELECT FIRST_NAME, LAST_NAME, EMAIL, PHONE, ID
    FROM AUTHORS
    WHERE EMAIL = $email
  """.query
}

/*
// https://blog.softwaremill.com/a-short-story-about-resource-handling-61b8405c352d
Bracket pattern: acquiring, using, and releasing various resources

IO(new BufferedReader(new FileReader(file))).bracket {
 in =>     
    IO(in.readLine())   
} { in =>    
   IO(in.close())   
}

*/

/*
Type projector:
https://github.com/typelevel/kind-projector

EitherT[?[_], Int, ?]    // equivalent to: type R[F[_], B] = EitherT[F, Int, B]
*/

class DoobieAuthorRepositoryInterpreter[F[_]: Bracket[?[_], Throwable]](val xa: Transactor[F])
extends AuthorRepositoryAlgebra[F]
with IdentityStore[F, Long, Author] { self =>
  import AuthorSQL._

  def findOne(id: Long): OptionT[F, Author] = OptionT(findOne(id).option.transact(xa))

  def findByEmail(email: String): OptionT[F, Author] =
    OptionT(find(email).option.transact(xa))
}


// ./Infrastruture/repository/doobie/DoobiePublicationRepositoryInterpreter.scala
private object PublicationSQL {
  /* We require conversion for date time */
  implicit val DateTimeMeta: Meta[Instant] =
    Meta[java.sql.Timestamp].imap(_.toInstant)(java.sql.Timestamp.from)

  def findOne(id: Long): Query0[Publication] = sql"""
    SELECT ID, AUTHOR_ID, PUBLICATION_DATE, TITLE
    FROM PUBLICATIONS
    WHERE ID = $id
  """.query[Publication]

  def find(authorId: Long): Query0[Publication] = sql"""
    SELECT ID, AUTHOR_ID, PUBLICATION_DATE, TITLE
    FROM PUBLICATIONS
    WHERE AUTHOR_ID = $authorId
  """.query[Publication]
  
}



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
