package io.github.pauljamescleary.petstore.repository

import cats._
import cats.implicits._
import doobie._
import doobie.implicits._
import io.github.pauljamescleary.petstore.model.{Order, OrderStatus}
import org.joda.time.DateTime

class DoobieOrderRepositoryInterpreter[F[_] : Monad](val xa: Transactor[F])
  extends OrderRepositoryAlgebra[F] {

  // This will clear the database.  Note, this would typically be done via something like FLYWAY (TODO)
  private val dropOrdersTable = dropOrdersDBTable(xa)

  // The tags column is controversial, could be a lookup table.  For our purposes, indexing on tags to allow searching is fine
  private val createOrdersTable = createOrdersDBTable(xa)

  /* We require type StatusMeta to handle our ADT Status */
  private implicit val StatusMeta: Meta[OrderStatus] =
    Meta[String].xmap(OrderStatus.apply, OrderStatus.nameOf)

  /* We require conversion for date time */
  private implicit val DateTimeMeta: Meta[DateTime] =
    Meta[java.sql.Timestamp].xmap(
      ts => new DateTime(ts.getTime),
      dt => new java.sql.Timestamp(dt.getMillis)
    )

  def migrate: F[Int] = {
    dropOrdersTable >> createOrdersTable
  }

  def put(order: Order): F[Order] = {
    val f: Fragment = sql"REPLACE INTO ORDERS (PET_ID, SHIP_DATE, STATUS, COMPLETE) values (${order.petId}, ${order.shipDate}, ${order.status}, ${order.complete})"
    val insert: ConnectionIO[Order] =
      for {
        id <- f.update.withUniqueGeneratedKeys[Long]("ID")
      } yield order.copy(id = Some(id))
    insert.transact(xa)
  }

  def get(orderId: Long): F[Option[Order]] = {
    val f: Fragment =
      sql"""
      SELECT PET_ID, SHIP_DATE, STATUS, COMPLETE
        FROM ORDERS
       WHERE ID = $orderId
     """
    f.query[Order].option.transact(xa)
  }

  def delete(orderId: Long): F[Option[Order]] = {
    get(orderId).flatMap {
      case Some(order) =>
        val f: Fragment = sql"DELETE FROM ORDERS WHERE ID = $orderId"
        f.update.run
          .transact(xa)
          .map(_ => Some(order))
      case None =>
        none[Order].pure[F]
    }
  }

  private def dropOrdersDBTable(xa: doobie.Transactor[F]) = {
    val f: Fragment =
      sql"""
    DROP TABLE IF EXISTS ORDERS
  """
    f.update.run.transact(xa)
  }

  private def createOrdersDBTable(xa: doobie.Transactor[F]) = {
    val f: Fragment =
      sql"""
    CREATE TABLE ORDERS (
      ID   SERIAL,
      PET_ID INT8 NOT NULL,
      SHIP_DATE TIMESTAMP NULL,
      STATUS VARCHAR NOT NULL,
      COMPLETE BOOLEAN NOT NULL
    )
  """
    f.update.run.transact(xa)
  }
}

object DoobieOrderRepositoryInterpreter {
  def apply[F[_] : Monad](
                           xa: Transactor[F]): DoobieOrderRepositoryInterpreter[F] = {
    new DoobieOrderRepositoryInterpreter(xa)
  }
}
