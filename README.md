# StreamFusion PostgreSQL Torznab Indexer

Indexeur Torznab pour **Prowlarr**, basé sur la table PostgreSQL `torrent_items` de StreamFusion.

Il permet à Prowlarr, Sonarr et Radarr d'effectuer des recherches directement dans la base PostgreSQL StreamFusion.

## Fonctionnement

Les torrents **sans TMDB et sans IMDb sont inclus**.

Les identifiants externes servent à accélérer et améliorer la précision des recherches, mais ne constituent jamais une condition obligatoire.

La recherche utilise notamment :

1. `imdb_id` / `tmdb_id` lorsqu'ils sont disponibles ;
2. `parsed_data.normalized_title` ;
3. `parsed_data.parsed_title` ;
4. `raw_title` ;
5. l'année lorsqu'elle est fournie ou détectée dans une recherche texte ;
6. les saisons et épisodes présents dans `parsed_data`, avec fallback `SxxEyy` dans le titre.

Les recherches texte de type :

```text
Le Grand Escogriffe 1976
```

peuvent également rechercher le titre sans l'année tout en conservant le contrôle de l'année.

Les résultats sont dédupliqués par `info_hash`, en privilégiant notamment les entrées ayant le meilleur classement et le plus de seeders.

## Filtrage AllDebrid

Le mode cache AllDebrid peut être activé afin de ne retourner à Prowlarr que les torrents disponibles instantanément sur AllDebrid.

Configuration recommandée :

```env
ALLDEBRID_CACHE_ONLY=true
ALLDEBRID_CHECK_LIMIT=30
ALLDEBRID_BATCH_SIZE=10
ALLDEBRID_MAX_CHECK=30
```

Lorsque ce mode est actif, les limites Torznab sont automatiquement réduites afin de limiter le nombre de vérifications AllDebrid.

## Image Docker

L'image officielle est disponible sur Docker Hub :

```text
laster13/streamfusion-torznab-indexer:latest
```

Pour télécharger l'image :

```bash
docker pull laster13/streamfusion-torznab-indexer:latest
```

## Installation

Créer la configuration locale :

```bash
cp .env.example .env
nano .env
```

Construire et démarrer le service :

```bash
docker compose up -d --build
```

Vérifier son état :

```bash
docker compose ps
```

## Tests

Depuis l'hôte Docker :

```bash
curl "http://127.0.0.1:8787/health"
```

Tester les capacités Torznab :

```bash
curl "http://127.0.0.1:8787/torznab/api?t=caps&apikey=YOUR_API_KEY"
```

Tester une recherche générique :

```bash
curl "http://127.0.0.1:8787/torznab/api?t=search&q=Dr%20Stone&apikey=YOUR_API_KEY"
```

Tester une recherche série :

```bash
curl "http://127.0.0.1:8787/torznab/api?t=tvsearch&q=Dr%20Stone&season=4&ep=12&apikey=YOUR_API_KEY"
```

`YOUR_API_KEY` doit être remplacé par la valeur de `INDEXER_API_KEY` définie dans `.env`.

Ne jamais publier le fichier `.env` ni une véritable clé API dans le dépôt Git.

## Prowlarr

Ajouter un indexeur **Generic Torznab**.

Lorsque Prowlarr partage le même réseau Docker que `sf-torznab` :

```text
URL : http://sf-torznab:8080/torznab
API Path : /api
API Key : valeur de INDEXER_API_KEY
```

L'URL Torznab complète utilisée en interne est donc :

```text
http://sf-torznab:8080/torznab/api
```

Pour un accès depuis l'hôte Docker :

```text
http://127.0.0.1:8787/torznab/api
```

Les recherches RSS, automatiques et interactives peuvent être activées dans Prowlarr.

## PostgreSQL

Configurer `DATABASE_URL` dans `.env` avec une adresse PostgreSQL accessible depuis le conteneur `sf-torznab`.

Exemple :

```env
DATABASE_URL=postgresql+asyncpg://streamfusion:PASSWORD@postgresql:5432/streamfusion
```

L'indexeur utilise notamment la table :

```text
torrent_items
```

Il ne possède pas de base PostgreSQL indépendante et ne stocke pas les torrents lui-même.

## Limites

Les paramètres principaux sont configurables dans `.env` :

```env
DEFAULT_LIMIT=100
MAX_LIMIT=200
```

Lorsque `ALLDEBRID_CACHE_ONLY=true`, l'indexeur applique actuellement des limites plus strictes :

```text
default : 10
maximum : 30
```

afin de limiter les appels vers AllDebrid.

## Performances

Les recherches génériques utilisent une pré-sélection limitée de candidats avant la déduplication afin d'éviter de trier plusieurs centaines de milliers de lignes PostgreSQL.

Les recherches par titre, TMDB et IMDb utilisent leur logique dédiée.

Le fichier :

```text
sql/recommended_indexes.sql
```

contient des index PostgreSQL optionnels pouvant améliorer certaines recherches.

Toujours vérifier les index déjà présents avant d'en créer de nouveaux.

## Sécurité

Le fichier `.env` doit rester local et être exclu de Git.

Les éléments suivants ne doivent jamais être publiés :

* `DATABASE_URL` contenant un mot de passe ;
* `INDEXER_API_KEY` ;
* `ALLDEBRID_API_KEY` ;
* toute autre clé ou information d'authentification.

Une clé aléatoire forte peut être générée avec :

```bash
openssl rand -hex 32
```
