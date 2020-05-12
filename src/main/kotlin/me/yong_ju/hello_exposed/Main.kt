package me.yong_ju.hello_exposed

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

object StarWarsFilms : IntIdTable() {
    val sequelId: Column<Int> = integer("sequel_id").uniqueIndex()
    val name: Column<String> = varchar("name", 50)
    val director: Column<String> = varchar("director", 50)
}

class StarWarsFilm(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<StarWarsFilm>(StarWarsFilms)

    var sequelId by StarWarsFilms.sequelId
    var name by StarWarsFilms.name
    var director by StarWarsFilms.director

    // many-to-one reference
    val ratings by UserRating referrersOn UserRatings.film

    // many-to-many reference
    val actors by Actor via StarWarsFilmActors
}

object Players : Table() {
    val sequelId: Column<Int> = integer("sequel_id").uniqueIndex()
    val name: Column<String> = varchar("name", 50)
}

object Cities : IntIdTable() {
    val name = varchar("name", 50)
}


//
// Referencing

object Users : IntIdTable() {
    val name = varchar("name", 50)
}

class User(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<User>(Users)

    var name by Users.name
}

object UserRatings : IntIdTable() {
    val value = long("value")

    // many-to-one reference
    val film = reference("film", StarWarsFilms)
    val user = reference("user", Users)

    // Optional reference
    val secondUser = reference("secondUser", Users).nullable()
}

class UserRating(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<UserRating>(UserRatings)

    var value by UserRatings.value

    // many-to-one reference
    var film by StarWarsFilm referencedOn UserRatings.film
    var user by User referencedOn UserRatings.user

    // Optional reference
    var secondUser by User optionalReferencedOn UserRatings.secondUser
}

// many-to-many reference
object Actors : IntIdTable() {
    val firstname = varchar("firstname", 50)
    val lastname = varchar("lastname", 50)
}

class Actor(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<Actor>(Actors)

    var firstname by Actors.firstname
    var lastname by Actors.lastname
}

object StarWarsFilmActors : Table() {
    val starWarsFilm = reference("starWarsFilm", StarWarsFilms)
    val actor = reference("actor", Actors)
    override val primaryKey = PrimaryKey(starWarsFilm, actor, name = "OK_StarWarsFilmActors_swf_act")
}

// Parent-Child reference
object NodeTable : IntIdTable() {
    val name = varchar("name", 50)
}

object NodeToNodes : Table() {
    val parent = reference("parent_node_id", NodeTable)
    val child = reference("child_user_id", NodeTable)
}

class Node(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<Node>(NodeTable)

    var name by NodeTable.name
    var parents by Node.via(NodeToNodes.child, NodeToNodes.parent)
    var children by Node.via(NodeToNodes.parent, NodeToNodes.child)
}

@Suppress("UnusedMainParameter", "UNUSED_VARIABLE")
fun main(args: Array<String>) {
    // an example connection to H2 DB
    Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")

    transaction {
        // print sql to std-out
        addLogger(StdOutSqlLogger)

        SchemaUtils.create(StarWarsFilms, Players, Cities)

        //
        // Basic CRUD operations

        // Create
        run {
            val id = StarWarsFilms.insertAndGetId {
                it[name] = "The Last Jedi"
                it[sequelId] = 8
                it[director] = "Rian Johnson"
            }
        }

        // Read
        run {
            val query: Query = StarWarsFilms.select { StarWarsFilms.sequelId eq 8 }

        }

        run {
            StarWarsFilms.select { StarWarsFilms.sequelId eq 8 }
                    .forEach { println(it[StarWarsFilms.name]) }
        }

        run {
            val filmAndDirector = StarWarsFilms.slice(StarWarsFilms.name, StarWarsFilms.director)
                    .selectAll()
                    .map { it[StarWarsFilms.name] to it[StarWarsFilms.director] }

        }

        run {
            val directors = StarWarsFilms.slice(StarWarsFilms.director)
                    .select { StarWarsFilms.sequelId less 5 }
                    .withDistinct()
                    .map { it[StarWarsFilms.director] }
        }

        // Update
        run {
            StarWarsFilms.update({ StarWarsFilms.sequelId eq 8 }) {
                it[name] = "Episode VIII - The Last Jedi"
            }
        }

        run {
            StarWarsFilms.update({ StarWarsFilms.sequelId eq 8 }) {
                with(SqlExpressionBuilder) {
                    it.update(sequelId, sequelId + 1)
                    // or
//                    it[sequelId] = sequelId + 1
                }
            }
        }

        // Delete
        run {
            StarWarsFilms.deleteWhere { StarWarsFilms.sequelId eq 8 }
        }


        //
        // Count

        run {
            val count = StarWarsFilms.select { StarWarsFilms.sequelId eq 8 }.count()
        }


        //
        // Order-by

        run {
            StarWarsFilms.selectAll().orderBy(StarWarsFilms.sequelId to SortOrder.ASC)
        }


        //
        // Group-by

        // Available functions are:
        // - count
        // - sum
        // - max
        // - min
        // - average
        run {
            StarWarsFilms.slice(StarWarsFilms.sequelId.count(), StarWarsFilms.director)
                    .selectAll()
                    .groupBy(StarWarsFilms.director)
        }


        //
        // Join

//        run {
//            (Players innerJoin StarWarsFilms).slice(Players.name.count(), StarWarsFilms.name)
//                    .select { StarWarsFilms.sequelId eq Players.sequelId }
//                    .groupBy(StarWarsFilms.name)
//        }

        run {
            Players.join(StarWarsFilms, JoinType.INNER, additionalConstraint = { StarWarsFilms.sequelId eq Players.sequelId })
                    .slice(Players.name.count(), StarWarsFilms.name)
                    .selectAll()
                    .groupBy(StarWarsFilms.name)
        }


        //
        // Alias

        run {
            val filmTable1 = StarWarsFilms.alias("ft1")
            filmTable1.selectAll()
        }

        run {
            val sequelTable = StarWarsFilms.alias("sql")
            val originalAndSequelNames = StarWarsFilms.innerJoin(sequelTable, { sequelId }, { sequelTable[StarWarsFilms.id] })
                    .slice(StarWarsFilms.name, sequelTable[StarWarsFilms.name])
                    .selectAll()
                    .map { it[StarWarsFilms.name] to it[sequelTable[StarWarsFilms.name]] }
        }


        //
        // Schema

        run {
            val schema = Schema("my_schema")
            // Creates a Schema
            SchemaUtils.createSchema(schema)
            // Drops a Schema
            SchemaUtils.dropSchema(schema)
        }


        //
        // Sequence

        // Define a Sequence
        run {
            val myseq = Sequence("my_sequence") // my_sequence is the sequence name.

            // Create and Drop a Sequence
            // Creates a sequence
            SchemaUtils.createSequence(myseq)
            // Drops a sequence
            SchemaUtils.dropSequence(myseq)

            // Use the NextVal function
//            val nextVal = myseq.nextVal()
//            val id = StarWarsFilms.insertAndGetId {
//                it[id] = nextVal
//                it[name] = "The Last Jedi"
//                it[sequelId] = 8
//                it[director] = "Rian Johnson"
//            }
        }


        //
        // Batch Insert

        run {
            val cityNames = listOf("Paris", "Moscow", "Helsinki")
            // NOTE: The batchInsert function will still create multiple INSERT statements when
            //       interacting with your database. You most likely want to couple this with the
            //       rewriteBatchInserts=true (or rewriteBatchedStatements=true) option of your relevant
            //       JDBC driver, which will convert those into a single bulkInsert.
            val allCitiesID = Cities.batchInsert(cityNames) { name ->
                this[Cities.name] = name
            }
        }
    }
}
