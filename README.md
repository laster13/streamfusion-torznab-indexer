# StreamFusion Torznab Indexer

Passerelle Torznab pour **Prowlarr, Sonarr et Radarr**, utilisant les données de StreamFusion.

Le projet fournit deux indexeurs Torznab complémentaires :

- **PostgreSQL**
- **Meilisearch**

Les vérifications de disponibilité instantanée sur **AllDebrid** sont centralisées par un broker interne afin de partager le cache, éviter les vérifications inutiles et limiter la pression sur l'API AllDebrid.

---

# Architecture

Le projet utilise :

```text
1 dépôt
1 Dockerfile
1 requirements.txt
1 docker-compose.yml
1 .env
1 image Docker
```

La même image Docker contient les trois applications Python.

Elle est exécutée dans **3 conteneurs distincts** :

| Conteneur | Rôle | Port hôte |
|---|---|---:|
| `sf-torznab` | Indexeur PostgreSQL Torznab | `8787` |
| `sf-torznab-meili` | Indexeur Meilisearch Torznab | `8788` |
| `sf-alldebrid-broker` | Broker AllDebrid interne | aucun |

Il ne s'agit donc **pas d'un seul conteneur exécutant trois processus**.

Chaque service possède son propre processus Uvicorn.

## Image Docker

Image prévue pour la publication :

```text
laster13/streamfusion-torznab-indexer:latest
```

Architecture de l'image :

```text
linux/amd64
linux/arm64
```

---

# Schéma

```text
                         ┌─────────────────────┐
                         │      Prowlarr       │
                         └──────────┬──────────┘
                                    │
                  ┌─────────────────┴─────────────────┐
                  │                                   │
                  ▼                                   ▼
        ┌───────────────────┐              ┌──────────────────────┐
        │    sf-torznab     │              │ sf-torznab-meili    │
        │    PostgreSQL     │              │    Meilisearch      │
        │    port 8787      │              │    port 8788         │
        └─────────┬─────────┘              └──────────┬───────────┘
                  │                                   │
                  └─────────────────┬─────────────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ sf-alldebrid-broker │
                         │   réseau interne    │
                         └──────────┬───────────┘
                                    │
                          ┌─────────┴─────────┐
                          │                   │
                          ▼                   ▼
                        Redis             AllDebrid
```

Les indexeurs conservent également un **fallback local AllDebrid** si le broker devient temporairement indisponible.

---

# Services

## 1. `sf-torznab`

Indexeur Torznab utilisant directement PostgreSQL StreamFusion.

Application Python :

```text
app.main:app
```

Port interne :

```text
8080
```

Port publié sur l'hôte :

```text
8787
```

URL depuis l'hôte :

```text
http://127.0.0.1:8787
```

---

## 2. `sf-torznab-meili`

Indexeur Torznab utilisant Meilisearch.

Application Python :

```text
app.meili_main:app
```

Port interne :

```text
8080
```

Port publié sur l'hôte :

```text
8788
```

URL depuis l'hôte :

```text
http://127.0.0.1:8788
```

---

## 3. `sf-alldebrid-broker`

Broker interne chargé de centraliser les vérifications AllDebrid.

Application Python :

```text
app.alldebrid_broker:app
```

Le broker :

- n'expose aucun port sur l'hôte ;
- communique avec les indexeurs sur le réseau Docker ;
- utilise Redis ;
- peut lire les disponibilités positives déjà présentes ;
- regroupe les vérifications AllDebrid ;
- évite plusieurs vérifications simultanées du même hash ;
- gère le nettoyage de ses magnets temporaires ;
- applique le quota global AllDebrid.

---

# PostgreSQL

L'indexeur PostgreSQL utilise la base StreamFusion.

La connexion est fournie par :

```text
DATABASE_URL
```

La table principale utilisée est :

```text
torrent_items
```

Le projet **ne possède pas sa propre base de torrents**.

Il exploite notamment :

- `imdb_id`
- `tmdb_id`
- `parsed_data.normalized_title`
- `parsed_data.parsed_title`
- `raw_title`
- année
- saison
- épisode
- `info_hash`
- seeders
- taille
- type

Les torrents sans TMDB ou IMDb peuvent également être retournés.

Les identifiants externes améliorent la précision mais ne sont pas obligatoires.

---

# Meilisearch

L'indexeur Meilisearch interroge l'index :

```text
torrents
```

Le Compose actuel attend Meilisearch à l'adresse interne :

```text
http://sfr-meilisearch-dev:7700
```

Une clé de recherche dédiée doit être fournie avec :

```text
MEILI_TORZNAB_SEARCH_KEY
```

Cette clé devrait idéalement être limitée aux opérations de recherche nécessaires.

---

# AllDebrid

## Mode cache-only

Lorsque :

```env
ALLDEBRID_CACHE_ONLY=true
```

les indexeurs ne retournent que les torrents considérés comme disponibles instantanément sur AllDebrid.

Paramètres disponibles :

```env
ALLDEBRID_CHECK_LIMIT=30
ALLDEBRID_BATCH_SIZE=10
ALLDEBRID_MAX_CHECK=30
```

---

# Broker AllDebrid

Le broker centralise les appels effectués par les deux indexeurs.

Fonctions principales :

| Fonction | Description |
|---|---|
| Cache positif Redis | évite de revérifier rapidement un hash connu |
| Cache StreamFusion | réutilise les disponibilités positives connues |
| PostgreSQL | peut réutiliser les informations positives disponibles |
| Single-flight | évite plusieurs vérifications simultanées du même hash |
| Batching | groupe les hashes |
| Cleanup | nettoie les magnets temporaires du broker |
| Capacity recovery | récupère de la capacité si AllDebrid refuse de nouveaux magnets |
| Authentification | protège le broker avec un token interne |

Le broker ne doit supprimer **que les magnets temporaires qu'il a lui-même créés**.

Il ne doit pas supprimer :

- les magnets de l'utilisateur ;
- les magnets créés par StreamFusion ;
- les magnets appartenant à d'autres applications.

Le broker **n'écrit pas dans `debrid_cache` de StreamFusion**.

---

# Quota AllDebrid

Les trois services de ce projet utilisent un limiteur Redis partagé.

Configuration actuelle :

```text
8 requêtes / seconde
450 requêtes / minute
```

Services concernés :

```text
sf-torznab
sf-torznab-meili
sf-alldebrid-broker
```

Ce mécanisme permet d'éviter que plusieurs conteneurs dépassent indépendamment les limites AllDebrid.

## Important

**StreamFusion lui-même ne participe pas à ce quota partagé.**

Si StreamFusion utilise la même clé AllDebrid simultanément, ses appels viennent donc s'ajouter aux appels de ce projet.

---

# Redis

Le broker et le quota global utilisent Redis.

Instance actuellement attendue :

```text
redis://sfr-redis-dev:6379/0
```

Clé de cleanup :

```text
sf:torznab:alldebrid:cleanup
```

Clé du quota :

```text
sf:alldebrid:global-quota:requests
```

---

# Prérequis

Les services suivants doivent déjà exister :

- PostgreSQL StreamFusion ;
- Redis ;
- Meilisearch ;
- réseau Docker StreamFusion ;
- éventuellement `traefik_proxy`.

Le projet **ne crée pas PostgreSQL, Redis ou Meilisearch**.

---

# Installation

Cloner le dépôt :

```bash
git clone https://github.com/laster13/streamfusion-torznab-indexer.git
cd streamfusion-torznab-indexer
```

Créer le fichier de configuration :

```bash
cp .env.example .env
nano .env
```

Le fichier :

```text
.env
```

contient les véritables clés et mots de passe.

**Il ne doit jamais être ajouté à Git.**

---

# Configuration `.env`

Le fichier `.env.example` contient les variables nécessaires au Compose.

## Docker

```env
TORZNAB_IMAGE=laster13/streamfusion-torznab-indexer:latest
TORZNAB_NETWORK=sfr-dev
```

`TORZNAB_IMAGE` correspond à l'image utilisée par les trois conteneurs.

`TORZNAB_NETWORK` correspond au réseau Docker permettant notamment à l'indexeur Meilisearch de communiquer avec le broker.

---

# PostgreSQL

```env
DATABASE_URL=postgresql+asyncpg://streamfusion:PASSWORD@postgresql-dev:5432/streamfusion

DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20
```

Adapter impérativement :

```text
PASSWORD
postgresql-dev
streamfusion
```

à l'environnement utilisé.

---

# Indexeur PostgreSQL

```env
INDEXER_NAME=StreamFusion PostgreSQL
INDEXER_API_KEY=change_me

DEFAULT_LIMIT=100
MAX_LIMIT=200
```

`INDEXER_API_KEY` protège l'accès à l'indexeur PostgreSQL.

Une clé forte peut être générée avec :

```bash
openssl rand -hex 32
```

---

# Indexeur Meilisearch

```env
MEILI_INDEXER_API_KEY=change_me
MEILI_TORZNAB_SEARCH_KEY=change_me
```

`MEILI_INDEXER_API_KEY` est la clé Torznab utilisée par Prowlarr.

`MEILI_TORZNAB_SEARCH_KEY` est la clé permettant à l'application d'effectuer les recherches dans Meilisearch.

---

# AllDebrid

```env
ALLDEBRID_API_KEY=change_me

ALLDEBRID_CACHE_ONLY=true
ALLDEBRID_CHECK_LIMIT=30
ALLDEBRID_BATCH_SIZE=10
ALLDEBRID_MAX_CHECK=30
```

`ALLDEBRID_API_KEY` ne doit jamais être publié.

---

# Authentification du broker

```env
ALLDEBRID_BROKER_TOKEN=change_me
```

Cette valeur doit être un secret aléatoire fort.

Exemple :

```bash
openssl rand -hex 32
```

Le même token est transmis :

- au broker ;
- à l'indexeur PostgreSQL ;
- à l'indexeur Meilisearch.

Le broker n'est pas destiné à être exposé publiquement.

---

# Liste complète des variables

Le Compose utilise actuellement exactement ces variables :

```text
ALLDEBRID_API_KEY
ALLDEBRID_BATCH_SIZE
ALLDEBRID_BROKER_TOKEN
ALLDEBRID_CACHE_ONLY
ALLDEBRID_CHECK_LIMIT
ALLDEBRID_MAX_CHECK
DATABASE_URL
DB_MAX_OVERFLOW
DB_POOL_SIZE
DEFAULT_LIMIT
INDEXER_API_KEY
INDEXER_NAME
MAX_LIMIT
MEILI_INDEXER_API_KEY
MEILI_TORZNAB_SEARCH_KEY
TORZNAB_IMAGE
TORZNAB_NETWORK
```

Soit :

```text
17 variables
```

---

# Démarrage depuis Docker Hub

Télécharger l'image :

```bash
docker compose pull
```

Démarrer les trois services :

```bash
docker compose up -d
```

Vérifier :

```bash
docker compose ps
```

Les trois conteneurs attendus sont :

```text
sf-torznab
sf-torznab-meili
sf-alldebrid-broker
```

---

# Construction locale

Pour construire l'image depuis les sources :

```bash
docker build \
  -t streamfusion-torznab-indexer:local \
  .
```

Puis modifier temporairement `.env` :

```env
TORZNAB_IMAGE=streamfusion-torznab-indexer:local
```

et recréer les services :

```bash
docker compose up -d --force-recreate
```

---

# Configuration Prowlarr

Les deux indexeurs doivent être créés **séparément** dans Prowlarr.

Type :

```text
Generic Torznab
```

---

## Indexeur PostgreSQL

Lorsque Prowlarr partage le réseau Docker :

```text
Name      : StreamFusion PostgreSQL
URL       : http://sf-torznab:8080/torznab
API Path  : /api
API Key   : valeur de INDEXER_API_KEY
```

URL Torznab complète :

```text
http://sf-torznab:8080/torznab/api
```

Depuis l'hôte :

```text
http://127.0.0.1:8787/torznab/api
```

---

## Indexeur Meilisearch

Lorsque Prowlarr partage le réseau Docker :

```text
Name      : StreamFusion Meilisearch
URL       : http://sf-torznab-meili:8080/torznab
API Path  : /api
API Key   : valeur de MEILI_INDEXER_API_KEY
```

URL Torznab complète :

```text
http://sf-torznab-meili:8080/torznab/api
```

Depuis l'hôte :

```text
http://127.0.0.1:8788/torznab/api
```

Les deux indexeurs peuvent être activés simultanément.

---

# Tests

## PostgreSQL — capacités

```bash
curl \
  "http://127.0.0.1:8787/torznab/api?t=caps&apikey=YOUR_POSTGRES_API_KEY"
```

---

## Meilisearch — capacités

```bash
curl \
  "http://127.0.0.1:8788/torznab/api?t=caps&apikey=YOUR_MEILI_API_KEY"
```

---

## Recherche PostgreSQL

```bash
curl \
  "http://127.0.0.1:8787/torznab/api?t=search&q=Iron%20Man%203&apikey=YOUR_POSTGRES_API_KEY"
```

---

## Recherche Meilisearch

```bash
curl \
  "http://127.0.0.1:8788/torznab/api?t=search&q=Iron%20Man%203&apikey=YOUR_MEILI_API_KEY"
```

---

# Vérification du broker

Le broker ne publie pas de port sur l'hôte.

Test depuis son conteneur :

```bash
docker exec sf-alldebrid-broker \
  python -c 'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8080/health").read().decode())'
```

Une réponse saine contient notamment :

```json
{
  "status": "ok",
  "redis": true,
  "postgres": true,
  "alldebrid_key": true
}
```

---

# Vérification cleanup AllDebrid

```bash
docker exec sfr-redis-dev \
  redis-cli ZCARD \
  sf:torznab:alldebrid:cleanup
```

La file peut augmenter temporairement pendant ou juste après des recherches.

Elle doit ensuite revenir à :

```text
0
```

---

# Logs

PostgreSQL :

```bash
docker logs --tail 200 sf-torznab
```

Meilisearch :

```bash
docker logs --tail 200 sf-torznab-meili
```

Broker :

```bash
docker logs --tail 200 sf-alldebrid-broker
```

---

# Mise à jour

Pour utiliser une nouvelle version publiée :

```bash
docker compose pull
docker compose up -d
```

Puis :

```bash
docker compose ps
```

---

# Sécurité

Ne jamais publier :

```text
.env
ALLDEBRID_API_KEY
ALLDEBRID_BROKER_TOKEN
INDEXER_API_KEY
MEILI_INDEXER_API_KEY
MEILI_TORZNAB_SEARCH_KEY
mot de passe contenu dans DATABASE_URL
```

Le fichier public de configuration est :

```text
.env.example
```

Il ne doit contenir que des exemples et des valeurs factices.

---

# Structure du dépôt

```text
.
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── meili_main.py
│   ├── alldebrid_broker.py
│   └── alldebrid_global_quota.py
├── .env.example
├── .github/
│   └── workflows/
│       └── docker-publish.yml
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── README.md
└── requirements.txt
```

---

# Rôle des fichiers principaux

| Fichier | Rôle |
|---|---|
| `app/main.py` | indexeur PostgreSQL |
| `app/meili_main.py` | indexeur Meilisearch |
| `app/alldebrid_broker.py` | broker central AllDebrid |
| `app/alldebrid_global_quota.py` | quota Redis partagé |
| `docker-compose.yml` | lancement des trois services |
| `Dockerfile` | construction de l'image commune |
| `.env.example` | exemple de configuration publique |

---

# Publication Docker

Le workflow GitHub Actions construit une image multi-architecture :

```text
linux/amd64
linux/arm64
```

Les registries prévus sont :

```text
Docker Hub
GitHub Container Registry
```

L'image contient toutes les applications.

Le service réellement exécuté est choisi dans `docker-compose.yml` :

```text
app.main:app
app.meili_main:app
app.alldebrid_broker:app
```

---

# Résumé

```text
1 dépôt
1 Dockerfile
1 requirements.txt
1 docker-compose.yml
1 image Docker

3 conteneurs
├── sf-torznab
├── sf-torznab-meili
└── sf-alldebrid-broker

2 indexeurs Prowlarr
├── PostgreSQL : port 8787
└── Meilisearch : port 8788

1 broker AllDebrid
├── pas de port hôte
├── cache partagé
├── quota partagé
├── single-flight
├── batching
└── cleanup des magnets temporaires
```
