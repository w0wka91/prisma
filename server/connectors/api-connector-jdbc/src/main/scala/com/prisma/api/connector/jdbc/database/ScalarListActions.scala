package com.prisma.api.connector.jdbc.database

import java.sql.PreparedStatement

import com.prisma.api.connector.Filter
import com.prisma.gc_values.{IdGCValue, ListGCValue}
import com.prisma.shared.models.{Model, ScalarField}
import slick.dbio.DBIOAction
import slick.jdbc.{JdbcBackend, PositionedParameters}

trait ScalarListActions extends BuilderBase with FilterConditionBuilder {
  import slickDatabase.profile.api._

  def deleteScalarListValuesByNodeIds(model: Model, ids: Vector[IdGCValue]): DBIO[Unit] = {
    val actions = model.scalarListFields.map { listField =>
      val query = sql
        .deleteFrom(scalarListTable(listField))
        .where(scalarListColumn(listField, "nodeId").in(placeHolders(ids)))

      deleteToDBIO(query)(
        setParams = pp => ids.foreach(pp.setGcValue)
      )
    }
    DBIO.seq(actions: _*)
  }

  def createScalarListValuesForNodeId(model: Model, id: IdGCValue, listFieldMap: Vector[(String, ListGCValue)]): DBIO[Unit] = {
    if (listFieldMap.isEmpty) {
      return DBIOAction.successful(())
    }

    setScalarListValues(model, listFieldMap, Vector(id))
    createScalarListValues(model, listFieldMap, Vector(id))
  }

  def updateScalarListValuesForNodeId(model: Model, id: IdGCValue, listFieldMap: Vector[(String, ListGCValue)]): DBIO[Unit] = {
    if (listFieldMap.isEmpty) {
      return DBIOAction.successful(())
    }

    setScalarListValues(model, listFieldMap, Vector(id))
  }

  def updateScalarListValuesByFilter(model: Model, listFieldMap: Vector[(String, ListGCValue)], whereFilter: Option[Filter]): DBIO[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    if (listFieldMap.isEmpty) {
      return DBIOAction.successful(())
    }

    val condition    = buildConditionForFilter(whereFilter)
    val aliasedTable = modelTable(model).as(topLevelAlias)
    val query = sql
      .select(aliasColumn(model.idField_!))
      .from(aliasedTable)
      .where(condition)

    val idQuery = queryToDBIO(query)(
      setParams = pp => SetParams.setFilter(pp, whereFilter),
      readResult = rs => rs.readWith(readNodeId(model))
    )

    for {
      ids <- idQuery
      _   <- setScalarListValues(model, listFieldMap, ids)
    } yield ()
  }

  private def createScalarListValues(model: Model, listFieldMap: Vector[(String, ListGCValue)], ids: Vector[IdGCValue]): DBIO[Unit] = {
    if (ids.isEmpty) {
      return DBIOAction.successful(())
    }

    SimpleDBIO[Unit] { ctx =>
      listFieldMap.foreach {
        case (fieldName, listGCValue) =>
          val scalarField = model.getScalarFieldByName_!(fieldName)
          insertListValueForIds(scalarField, listGCValue, ids, ctx)
      }
    }
  }

  private def setScalarListValues(model: Model, listFieldMap: Vector[(String, ListGCValue)], ids: Vector[IdGCValue]): DBIO[Unit] = {
    if (ids.isEmpty) {
      return DBIOAction.successful(())
    }

    SimpleDBIO[Unit] { ctx =>
      listFieldMap.foreach {
        case (fieldName, listGCValue) =>
          val scalarField = model.getScalarFieldByName_!(fieldName)
          deleteListValuesForIds(scalarField, ids, ctx)
          insertListValueForIds(scalarField, listGCValue, ids, ctx)
      }
    }
  }

  private def deleteListValuesForIds(scalarField: ScalarField, ids: Vector[IdGCValue], ctx: JdbcBackend#JdbcActionContext) = {
    val table = scalarListTable(scalarField)
    val wipe = sql
      .deleteFrom(table)
      .where(scalarListColumn(scalarField, nodeIdFieldName).in(placeHolders(ids)))
      .getSQL

    val wipeOldValues: PreparedStatement = ctx.connection.prepareStatement(wipe)
    val pp                               = new PositionedParameters(wipeOldValues)
    ids.foreach(pp.setGcValue)

    wipeOldValues.executeUpdate()
  }

  private def insertListValueForIds(scalarField: ScalarField, value: ListGCValue, ids: Vector[IdGCValue], ctx: JdbcBackend#JdbcActionContext) = {
    val table = scalarListTable(scalarField)
    val insert = sql
      .insertInto(table)
      .columns(
        scalarListColumn(scalarField, nodeIdFieldName),
        scalarListColumn(scalarField, positionFieldName),
        scalarListColumn(scalarField, valueFieldName)
      )
      .values(placeHolder, placeHolder, placeHolder)
      .getSQL

    val insertNewValues: PreparedStatement = ctx.connection.prepareStatement(insert)
    val newValueTuples                     = valueTuplesForListField(ids, value)

    newValueTuples.foreach { tuple =>
      insertNewValues.setGcValue(1, tuple._1)
      insertNewValues.setInt(2, tuple._2)
      insertNewValues.setGcValue(3, tuple._3)
      insertNewValues.addBatch()
    }
    insertNewValues.executeBatch()
  }

  private def valueTuplesForListField(ids: Vector[IdGCValue], listGCValue: ListGCValue) = {
    for {
      nodeId            <- ids
      (value, position) <- listGCValue.values.zip((1 to listGCValue.size).map(_ * 1000))
    } yield {
      (nodeId, position, value)
    }
  }
}
