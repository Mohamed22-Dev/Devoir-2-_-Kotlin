import kotlinx.coroutines.*
import kotlin.random.Random

data class Commande(
    val id: Int,
    val plats: List<String>,
    val total: Double
) {
    fun afficherDetails() {
        println("Commande #$id")
        println("Plats : ${plats.joinToString(", ")}")
        println("Total : $total DH")
    }
}

class Serveur(
    val nom: String) {
    private var compteur = 1



    fun prendreCommande(plats: List<String>, prix: List<Double>): Commande {
        require(plats.size == prix.size)
        val commande = Commande(compteur++, plats, prix.sum())
        println("Serveur $nom prend la commande #${commande.id}")
        return commande }

    fun afficherCommande(commandes: List<Commande>) {
        println("Commandes du serveur $nom :")
        if (commandes.isEmpty()) {
            println("Aucune commande.")
        } else {
            commandes.forEach {
                it.afficherDetails()
            } } } }

class Cuisinier(val nom: String) {

    suspend fun preparerPlat(plat: String): String {
        delay(1000)
        println("$nom prépare : $plat")
        return "$plat préparé"
    }

    suspend fun preparerCommande(
        commande: Commande
    ): List<String> {

        return coroutineScope {
            commande.plats.map { plat ->
                async {
                    preparerPlat(plat)
                }
            }.awaitAll()
        }
    }
}

class Cuisine(
    val cuisiniers: List<Cuisinier>
) {

    suspend fun gererPreparationCommande(
        commande: Commande
    ): List<String> {
        require(cuisiniers.isNotEmpty())
        return coroutineScope { commande.plats.mapIndexed { index, plat ->
            async {
                val cuisinier =
                    cuisiniers[index % cuisiniers.size]
                cuisinier.preparerPlat(plat)
            }
        }.awaitAll()
        }
    }
}

class Caisse {

    private val paiements =
        mutableMapOf<Int, Deferred<Boolean>>()

    suspend fun traiterPaiement(
        commande: Commande): Boolean {
        return coroutineScope { val paiement = async {
            delay(1000)
            if (Random.nextInt(100) < 20) { throw Exception("Échec du paiement") }
            println("Paiement de la commande #${commande.id} effectué.")
            true }
            paiements[commande.id] = paiement
            try { paiement.await() }
            finally { paiements.remove(commande.id) } } }

    fun annulerPaiement(commande: Commande) {

        paiements[commande.id]?.cancel()

        println(
            "Paiement de la commande #${commande.id} annulé."
        )
    }
}

class Restaurant(
    val commandes: MutableList<Commande>,
    val serveurs: List<Serveur>,
    val cuisine: Cuisine,
    val caisse: Caisse
) {

    suspend fun prendreCommandeEtTraiter(serveur: Serveur, plats: List<String>, prix: List<Double>) = coroutineScope {
        val commande = serveur.prendreCommande(plats, prix)
        commandes.add(commande)
        println(
            "Commande #${commande.id} reçue.")
        try { cuisine.gererPreparationCommande(
            commande)
            println("Commande #${commande.id} prête.")
            val paiement = async { caisse.traiterPaiement(commande) }
            paiement.await()
            println("Commande #${commande.id} terminée.")
        } catch (e: CancellationException) {
            println("Commande #${commande.id} annulée.")
        } catch (e: Exception) {
            println("Erreur commande #${commande.id} : ${e.message}"
            )
        }
    }

    fun afficherCommandesEnCours() {

        println("COMMANDES EN COURS ")

        if (commandes.isEmpty()) {
            println("Aucune commande en cours.")
        } else {
            commandes.forEach {
                it.afficherDetails()
            }
        }
    }
}


fun main() = runBlocking {
    val serveur1 = Serveur("Mohamed")
    val serveur2 = Serveur("Ali")
    val cuisinier1 = Cuisinier("Ahmed")
    val cuisinier2 = Cuisinier("Youssef")


    val cuisine = Cuisine(listOf(cuisinier1, cuisinier2))
    val caisse = Caisse()
    val commandes = mutableListOf<Commande>()
    val restaurant = Restaurant(
        commandes,
        listOf(serveur1, serveur2),
        cuisine,
        caisse
    )
    val commande = serveur1.prendreCommande(
        listOf("Pizza", "Burger"),
        listOf(50.0, 40.0)
    )


    commande.afficherDetails()


    serveur1.afficherCommande(listOf(commande))


    cuisinier1.preparerPlat("Pizza")


    cuisinier1.preparerCommande(commande)

    // Préparer la commande avec la cuisine
    cuisine.gererPreparationCommande(commande)

    // Traiter le paiement
    try {
        caisse.traiterPaiement(commande)
    } catch (e: Exception) {
        println("Erreur paiement : ${e.message}")
    }

    // Appeler annulerPaiement
    caisse.annulerPaiement(commande)

    // Traiter une commande avec le restaurant
    restaurant.prendreCommandeEtTraiter(
        serveur2,
        listOf("Tacos", "Frites"),
        listOf(35.0, 15.0)
    )

    // Afficher les commandes en cours
    restaurant.afficherCommandesEnCours()
}