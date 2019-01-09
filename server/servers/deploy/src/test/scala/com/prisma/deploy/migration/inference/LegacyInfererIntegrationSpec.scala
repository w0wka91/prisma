package com.prisma.deploy.migration.inference

import com.prisma.deploy.connector.InferredTables
import com.prisma.deploy.migration.validation.LegacyDataModelValidator
import com.prisma.deploy.specutils.DeploySpecBase
import com.prisma.shared.models.ConnectorCapability.{LegacyDataModelCapability, MigrationsCapability}
import com.prisma.shared.models._
import org.scalatest.{FlatSpec, Matchers}

class LegacyInfererIntegrationSpec extends FlatSpec with Matchers with DeploySpecBase {

  "they" should "should propose no UpdateRelation when ambiguous relations are involved" in {
    val schema =
      """
        |type Todo {
        |  comments1: [Comment] @relation(name: "TodoToComments1")
        |  comments2: [Comment] @relation(name: "TodoToComments2")
        |}
        |type Comment {
        |  text: String
        |  todo1: Todo @relation(name: "TodoToComments1")
        |  todo2: Todo @relation(name: "TodoToComments2")
        |}
      """.stripMargin
    val project = inferSchema(schema)
    val steps   = inferSteps(previousSchema = project, next = schema)

    steps should be(empty)
  }

  "they" should "only propose an UpdateRelation step when relation directives get removed" in {
    val previousSchema =
      """
        |type Todo {
        |  comments: [Comment] @relation(name: "ManualRelationName")
        |}
        |type Comment {
        |  text: String
        |  todo: Todo @relation(name: "ManualRelationName")
        |}
      """.stripMargin
    val project = inferSchema(previousSchema)

    val nextSchema =
      """
        |type Todo {
        |  comments: [Comment]
        |}
        |type Comment {
        |  text: String
        |  todo: Todo
        |}
      """.stripMargin
    val steps = inferSteps(previousSchema = project, next = nextSchema)

    steps should have(size(1))
    steps should be(
      Vector(
        UpdateRelation(
          name = "ManualRelationName",
          newName = Some("CommentToTodo")
        )
      ))

  }

  "they" should "not propose a DeleteRelation step when relation directives gets added" in {
    val previousSchema =
      """
        |type Todo {
        |  comments: [Comment]
        |}
        |type Comment {
        |  text: String
        |  todo: Todo
        |}
      """.stripMargin
    val project = inferSchema(previousSchema)

    val nextSchema =
      """
        |type Todo {
        |  comments: [Comment] @relation(name: "ManualRelationName")
        |}
        |type Comment {
        |  text: String
        |  todo: Todo @relation(name: "ManualRelationName")
        |}
      """.stripMargin
    val steps = inferSteps(previousSchema = project, next = nextSchema)

    steps should have(size(1))
    steps should be(
      Vector(
        UpdateRelation(
          name = "CommentToTodo",
          newName = Some("ManualRelationName")
        )
      ))
  }

  "they" should "not propose an Update and Delete at the same time when renaming a relation" in {
    val previousSchema =
      """
        |type Todo {
        |  comments: [Comment] @relation(name: "ManualRelationName1")
        |}
        |type Comment {
        |  text: String
        |  todo: Todo @relation(name: "ManualRelationName1")
        |}
      """.stripMargin
    val project = inferSchema(previousSchema)

    val nextSchema =
      """
        |type Todo {
        |  comments: [Comment] @relation(name: "ManualRelationName2")
        |}
        |type Comment {
        |  text: String
        |  todo: Todo @relation(name: "ManualRelationName2")
        |}
      """.stripMargin
    val steps = inferSteps(previousSchema = project, next = nextSchema)

    steps should have(size(2))
    steps should contain allOf (
      DeleteRelation("ManualRelationName1"),
      CreateRelation("ManualRelationName2")
    )
  }

  "they" should "handle ambiguous relations correctly" in {
    val previousSchema =
      """
        |type Todo {
        |  title: String
        |}
        |type Comment {
        |  text: String
        |}
      """.stripMargin
    val project = inferSchema(previousSchema)

    val nextSchema =
      """
        |type Todo {
        |  title: String
        |  comment1: Comment @relation(name: "TodoToComment1")
        |  comment2: Comment @relation(name: "TodoToComment2")
        |}
        |type Comment {
        |  text: String
        |  todo1: Todo @relation(name: "TodoToComment1")
        |  todo2: Todo @relation(name: "TodoToComment2")
        |}
      """.stripMargin
    val steps = inferSteps(previousSchema = project, next = nextSchema)
    steps should have(size(6))
    steps should contain allOf (
      CreateField(
        model = "Todo",
        name = "comment1"
      ),
      CreateField(
        model = "Todo",
        name = "comment2"
      ),
      CreateField(
        model = "Comment",
        name = "todo1"
      ),
      CreateField(
        model = "Comment",
        name = "todo2"
      ),
      CreateRelation(
        name = "TodoToComment1"
      ),
      CreateRelation(
        name = "TodoToComment2"
      )
    )
  }

  "they" should "not detect a change in the onDelete relation argument as it does not need to update the schema" in {
    val previousSchema =
      """
        |type Course {
        |  id: ID! @unique
        |	sections: [CourseSection] @relation(name: "CourseSections" onDelete: CASCADE)
        |}
        |
        |type CourseSection {
        |  id: ID! @unique
        |  course: Course! @relation(name: "CourseSections")
        |}
      """.stripMargin
    val project = inferSchema(previousSchema)
    val nextSchema =
      """
        |type Course {
        |  id: ID! @unique
        |	sections: [CourseSection] @relation(name: "CourseSections")
        |}
        |
        |type CourseSection {
        |  id: ID! @unique
        |  course: Course! @relation(name: "CourseSections")
        |}
      """.stripMargin
    val steps = inferSteps(previousSchema = project, next = nextSchema)
    steps should have(size(0))
  }

  def inferSchema(schema: String): Schema = {
    inferSchema(Schema(), schema)
  }

  def inferSchema(previous: Schema, schema: String): Schema = {
    val capabilities = ConnectorCapabilities(MigrationsCapability, LegacyDataModelCapability)
    val validator = LegacyDataModelValidator(
      schema,
      LegacyDataModelValidator.directiveRequirements,
      deployConnector.fieldRequirements,
      capabilities = capabilities
    )

    val prismaSdl = validator.generateSDL

    val nextSchema = SchemaInferrer(capabilities).infer(previous, SchemaMapping.empty, prismaSdl, InferredTables.empty)

//    println(s"Relations of infered schema:\n  " + nextSchema.relations)
    nextSchema
  }

  def inferSteps(previousSchema: Schema, next: String): Vector[MigrationStep] = {
    val nextSchema = inferSchema(previousSchema, next)
//    println(s"fields of next project:")
//    nextSchema.allFields.foreach(println)
    MigrationStepsInferrer().infer(
      previousSchema = previousSchema,
      nextSchema = nextSchema,
      renames = SchemaMapping.empty
    )
  }
}