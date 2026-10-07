
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

// ===================== Partie 1 : Produits et Stock =====================

class Produit(
    val id: Int,
    val nom: String,
    var quantite: Int
) {
    fun afficherDetails() {
        println("Produit #$id : $nom | Quantité : $quantite")
    }
}

// ===================== Partie 4 : Evénements (Flow) =====================

enum class TypeEvenement { PRODUIT_AJOUTE, QUANTITE_AJOUTEE, QUANTITE_RETIREE }

data class EvenementStock(
    val type: TypeEvenement,
    val idProduit: Int,
    val quantite: Int
)

class Stock {

    val produits = mutableMapOf<Int, Produit>()
    private val mutex = Mutex()

    private val _evenements = MutableSharedFlow<EvenementStock>(extraBufferCapacity = 64)
    val evenements: SharedFlow<EvenementStock> = _evenements

    suspend fun ajouterProduit(produit: Produit) {
        mutex.withLock {
            produits[produit.id] = produit }
        _evenements.emit(EvenementStock(TypeEvenement.PRODUIT_AJOUTE, produit.id, produit.quantite)) }
    suspend fun ajouterQuantite(idProduit: Int, quantite: Int) {
        delay(500)
        mutex.withLock {
            val produit = produits[idProduit]
                ?: throw IllegalArgumentException("Produit #$idProduit introuvable")
            produit.quantite += quantite }
        _evenements.emit(
            EvenementStock(TypeEvenement.QUANTITE_AJOUTEE, idProduit, quantite)) }



    suspend fun retirerQuantite(idProduit: Int, quantite: Int) {
        delay(500)
        mutex.withLock {
            val produit = produits[idProduit]
                ?: throw IllegalArgumentException("Produit #$idProduit introuvable")
            if (quantite > produit.quantite) {
    throw IllegalStateException("Stock insuffisant pour ${produit.nom} : demandé $quantite, disponible ${produit.quantite}") }
            produit.quantite -= quantite }
        _evenements.emit(
            EvenementStock(TypeEvenement.QUANTITE_RETIREE, idProduit, quantite)) }
    fun afficherStock() {
        println("=== STOCK ACTUEL ===")
        if (produits.isEmpty()) { println("Le stock est vide.")
        } else { produits.values.forEach { it.afficherDetails() }
        }
    }
}

// ===================== Partie 2 : Commandes Clients =====================

class Commande(
    val idCommande: Int,
    val produits: List<Pair<Int, Int>>
) {
    fun afficherCommande() {
        println("Commande #$idCommande")
        produits.forEach { (idProduit, quantite) ->
            println("  Produit #$idProduit | Quantité demandée : $quantite")
        }
    }
}

class GestionnaireCommandes(
    val stock: Stock,
    val commandes: MutableList<Commande> = mutableListOf()
) {

    suspend fun traiterCommande(commande: Commande) {
        println("Traitement de la commande #${commande.idCommande}...")
        commandes.add(commande)
        val manquant = commande.produits.firstOrNull { (idProduit, quantite) ->
            (stock.produits[idProduit]?.quantite ?: 0) < quantite
        }
        if (manquant != null) {
            println("Commande #${commande.idCommande} refusée : stock insuffisant pour le produit #${manquant.first}")
            return }
        try {
            for ((idProduit, quantite) in commande.produits) {
                stock.retirerQuantite(idProduit, quantite) }
            println("Commande #${commande.idCommande} traitée avec succès.")
        } catch (e: Exception) {
            println("Commande #${commande.idCommande} échouée : ${e.message}")
        }
    }

    suspend fun gererCommandes(commandes: List<Commande>) = coroutineScope {
        commandes.forEach { commande ->
            launch {
                traiterCommande(commande)
            }
        }
    }
}

// ===================== Partie 3 : Opérations asynchrones =====================

class Entrepot(val stock: Stock, val gestionnaireCommandes: GestionnaireCommandes
) {
    val inventaireTempsReel = mutableMapOf<Int, Int>()
    fun ajouterProduitAuStock(scope: CoroutineScope, produit: Produit): Job {
        return scope.launch { stock.ajouterProduit(produit)
            println("Produit ${produit.nom} ajouté au stock.")
        } }

    fun retirerProduitDuStock(scope: CoroutineScope, idProduit: Int, quantite: Int): Job {
        return scope.launch {
            try { stock.retirerQuantite(idProduit, quantite)
                println("Retrait de $quantite unité(s) du produit #$idProduit effectué.")
            } catch (e: Exception) {
                println("Erreur retrait : ${e.message}")
            } } }
    fun gererInventaire(scope: CoroutineScope): Job {
        return scope.launch {
            println(">>> Début de l'inventaire")
            delay(1000)
            stock.afficherStock()
            println(">>> Fin de l'inventaire")
        }
    }

    fun surveillerEvenements(scope: CoroutineScope): Job {
     return scope.launch(start = CoroutineStart.UNDISPATCHED) {
        stock.evenements.collect { evenement ->
            when (evenement.type) {
              TypeEvenement.PRODUIT_AJOUTE ->
                        inventaireTempsReel[evenement.idProduit] = evenement.quantite
      TypeEvenement.QUANTITE_AJOUTEE ->
                        inventaireTempsReel[evenement.idProduit] =
                            (inventaireTempsReel[evenement.idProduit] ?: 0) + evenement.quantite
          TypeEvenement.QUANTITE_RETIREE ->
                        inventaireTempsReel[evenement.idProduit] =
                            (inventaireTempsReel[evenement.idProduit] ?: 0) - evenement.quantite }
     println("[EVENEMENT] ${evenement.type} | produit #${evenement.idProduit} | " +
      "quantité ${evenement.quantite} | inventaire temps réel : ${inventaireTempsReel[evenement.idProduit]}"
                )
            }
        }
    }
}

// ===================== main =====================

fun main() = runBlocking {
    val stock = Stock()
    val entrepot = Entrepot(stock, GestionnaireCommandes(stock))
    val surveillance = entrepot.surveillerEvenements(this)
    entrepot.ajouterProduitAuStock(this, Produit(1, "Clavier", 50)).join()
    entrepot.ajouterProduitAuStock(this, Produit(2, "Ecran", 10)).join()
    entrepot.retirerProduitDuStock(this, 2, 100).join()

    val inventaire = entrepot.gererInventaire(this)
    entrepot.gestionnaireCommandes.gererCommandes(
        listOf(
            Commande(1, listOf(Pair(1, 10))),
            Commande(2, listOf(Pair(2, 50)))))
    inventaire.join()
    delay(200)
    surveillance.cancel()
}