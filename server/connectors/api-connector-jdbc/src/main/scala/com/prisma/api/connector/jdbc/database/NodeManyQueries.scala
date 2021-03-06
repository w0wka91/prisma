package com.prisma.api.connector.jdbc.database

import com.prisma.api.connector._
import com.prisma.gc_values.IdGCValue
import com.prisma.shared.models.{Model, RelationField}
import org.jooq.{Record, SelectForUpdateStep}
import slick.jdbc.PositionedParameters

trait NodeManyQueries extends BuilderBase with FilterConditionBuilder with CursorConditionBuilder with OrderByClauseBuilder with LimitClauseBuilder {
  import slickDatabase.profile.api._

  def getNodes(model: Model, args: Option[QueryArguments], selectedFields: SelectedFields): DBIO[ResolverResult[PrismaNode]] = {
    val query = modelQuery(model, args, selectedFields)

    queryToDBIO(query)(
      setParams = pp => SetParams.setQueryArgs(pp, args),
      readResult = { rs =>
        val result = rs.readWith(readsPrismaNode(model, selectedFields.scalarNonListFields))
        ResolverResult(args, result)
      }
    )
  }

  private def modelQuery(model: Model, queryArguments: Option[QueryArguments], selectedFields: SelectedFields): SelectForUpdateStep[Record] = {
    val condition       = buildConditionForFilter(queryArguments.flatMap(_.filter))
    val cursorCondition = buildCursorCondition(queryArguments, model)
    val order           = orderByForModel(model, topLevelAlias, queryArguments)
    val limit           = limitClause(queryArguments)
    val jooqFields      = selectedFields.scalarNonListFields.map(aliasColumn)

    val base = sql
      .select(jooqFields.toVector: _*)
      .from(modelTable(model).as(topLevelAlias))
      .where(condition, cursorCondition)
      .orderBy(order: _*)

    limit match {
      case Some(_) => base.limit(intDummy).offset(intDummy)
      case None    => base
    }
  }

  def getRelatedNodes(
      fromField: RelationField,
      fromNodeIds: Vector[IdGCValue],
      args: Option[QueryArguments],
      selectedFields: SelectedFields
  ): DBIO[Vector[ResolverResult[PrismaNodeWithParent]]] = {
    if (isMySql && args.exists(_.isWithPagination)) {
      selectAllFromRelatedWithPaginationForMySQL(fromField, fromNodeIds, args, selectedFields)
    } else {
      val builder = RelatedModelsQueryBuilder(slickDatabase, schemaName, fromField, args, fromNodeIds, selectedFields)
      val query   = if (args.exists(_.isWithPagination)) builder.queryWithPagination else builder.queryWithoutPagination

      queryToDBIO(query)(
        setParams = { pp =>
          fromNodeIds.foreach(pp.setGcValue)
          args.foreach { arg =>
            arg.filter.foreach(filter => SetParams.setFilter(pp, filter))

            SetParams.setCursor(pp, arg)

            if (arg.isWithPagination) {
              val params = limitClauseForWindowFunction(args)
              pp.setInt(params._1)
              pp.setInt(params._2)
            }
          }
        },
        readResult = { rs =>
          val result              = rs.readWith(readPrismaNodeWithParent(fromField, selectedFields.scalarNonListFields))
          val itemGroupsByModelId = result.groupBy(_.parentId)
          fromNodeIds.map { id =>
            itemGroupsByModelId.find(_._1 == id) match {
              case Some((_, itemsForId)) => ResolverResult(args, itemsForId, parentModelId = Some(id))
              case None                  => ResolverResult(Vector.empty[PrismaNodeWithParent], hasPreviousPage = false, hasNextPage = false, parentModelId = Some(id))
            }
          }
        }
      )
    }
  }

  private def selectAllFromRelatedWithPaginationForMySQL(
      fromField: RelationField,
      fromModelIds: Vector[IdGCValue],
      args: Option[QueryArguments],
      selectedFields: SelectedFields
  ): DBIO[Vector[ResolverResult[PrismaNodeWithParent]]] = {
    require(args.exists(_.isWithPagination))
    val builder = RelatedModelsQueryBuilder(slickDatabase, schemaName, fromField, args, fromModelIds, selectedFields)

    SimpleDBIO { ctx =>
      val baseQuery        = "(" + builder.mysqlHack.getSQL + ")"
      val distinctModelIds = fromModelIds.distinct
      val queries          = Vector.fill(distinctModelIds.size)(baseQuery)
      val query            = queries.mkString(" union all ")

      val ps = ctx.connection.prepareStatement(query)
      val pp = new PositionedParameters(ps)

      distinctModelIds.foreach { id =>
        pp.setGcValue(id)

        args.foreach { arg =>
          arg.filter.foreach(filter => SetParams.setFilter(pp, filter))

          SetParams.setCursor(pp, arg)

          if (arg.isWithPagination) {
            limitClause(args).foreach { params =>
              pp.setInt(params._1)
              pp.setInt(params._2)
            }
          }
        }
      }

      val rs                  = ps.executeQuery()
      val result              = rs.readWith(readPrismaNodeWithParent(fromField, selectedFields.scalarNonListFields))
      val itemGroupsByModelId = result.groupBy(_.parentId)
      fromModelIds.map { id =>
        itemGroupsByModelId.find(_._1 == id) match {
          case Some((_, itemsForId)) => ResolverResult(args, itemsForId, parentModelId = Some(id))
          case None                  => ResolverResult(Vector.empty[PrismaNodeWithParent], hasPreviousPage = false, hasNextPage = false, parentModelId = Some(id))
        }
      }
    }
  }
}
