package io.github.pauljamescleary.petstore.repository

import cats._
import cats.data._
import cats.implicits._
import doobie._
import doobie.implicits._
import io.github.pauljamescleary.petstore.model._

class DoobiePetRepositoryInterpreter[F[_] : Monad](val xa: Transactor[F])
  extends PetRepositoryAlgebra[F] {

  // This will clear the database.  Note, this would typically be done via something like FLYWAY (TODO)
  private val dropPetTable = dropPetDBTable(xa)

  // The tags column is controversial, could be a lookup table.  For our purposes, indexing on tags to allow searching is fine
  private val createPetTable = createPetDBTable(xa)

  /* We require type StatusMeta to handle our ADT Status */
  private implicit val StatusMeta: Meta[PetStatus] =
    Meta[String].xmap(PetStatus.apply, PetStatus.nameOf)

  /* This is used to marshal our sets of strings */
  private implicit val SetStringMeta: Meta[Set[String]] = Meta[String]
    .xmap(str => str.split(',').toSet, strSet => strSet.mkString(","))

  def migrate: F[Int] = {
    dropPetTable >> createPetTable
  }

  override def put(pet: Pet): F[Pet] = {
    val f: Fragment = f = sql"""REPLACE INTO PET (NAME, CATEGORY, BIO, STATUS, TAGS, PHOTO_URLS) values (${pet.name}, ${pet.category}, ${pet.bio}, ${pet.status}, ${pet.photoUrls}, ${pet.tags})"""

    val insert: ConnectionIO[Pet] =
      for {
        id <- f.update.withUniqueGeneratedKeys[Long]("ID")
      } yield pet.copy(id = Some(id))

    insert.transact(xa)
  }

  override def get(id: Long): F[Option[Pet]] = {
    val f: Fragment =
      sql"""
      SELECT NAME, CATEGORY, BIO, STATUS, TAGS, PHOTO_URLS, ID
        FROM PET
       WHERE ID = $id
     """
    f.query[Pet].option.transact(xa)
  }

  override def delete(id: Long): F[Option[Pet]] = {
    get(id).flatMap {
      case Some(pet) =>
        val f: Fragment = sql"DELETE FROM PET WHERE ID = $id"
        f.update.run
          .transact(xa)
          .map(_ => Some(pet))
      case None =>
        none[Pet].pure[F]
    }
  }

  override def findByNameAndCategory(name: String, category: String): F[Set[Pet]] = {
    val f: Fragment =
      sql"""SELECT NAME, CATEGORY, BIO, STATUS, TAGS, PHOTO_URLS, ID
            FROM PET
           WHERE NAME = $name AND CATEGORY = $category
           """
    f.query[Pet].list.transact(xa).map(_.toSet)
  }

  override def list(pageSize: Int, offset: Int): F[List[Pet]] = {
    val f: Fragment =
      sql"""SELECT NAME, CATEGORY, BIO, STATUS, TAGS, PHOTO_URLS, ID
            FROM PET
            ORDER BY NAME LIMIT $offset,$pageSize"""

    f.query[Pet]
      .list
      .transact(xa)
  }

  override def findByStatus(statuses: NonEmptyList[PetStatus]): F[List[Pet]] = {
    val f1: Fragment = sql"""SELECT NAME, CATEGORY, BIO, STATUS, TAGS, PHOTO_URLS, ID FROM PET WHERE """

    val q = f1 ++ Fragments.in(fr"STATUS", statuses)

    q.query[Pet].list.transact(xa)
  }

  private def createPetDBTable(xa: Transactor[F]): F[Int] = {
    val f: Fragment =
      sql"""
    CREATE TABLE PET (
      ID   SERIAL,
      NAME VARCHAR NOT NULL,
      CATEGORY VARCHAR NOT NULL,
      BIO  VARCHAR NOT NULL,
      STATUS VARCHAR NOT NULL,
      PHOTO_URLS VARCHAR NOT NULL,
      TAGS VARCHAR NOT NULL
    )
  """
    f.update.run.transact(xa)
  }

  private def dropPetDBTable(xa: Transactor[F]): F[Int] = {
    val f: Fragment = sql"""DROP TABLE IF EXISTS PET"""
    f.update.run.transact(xa)
  }
}

object DoobiePetRepositoryInterpreter {
  def apply[F[_] : Monad](
                           xa: Transactor[F]): DoobiePetRepositoryInterpreter[F] = {
    new DoobiePetRepositoryInterpreter(xa)
  }
}
