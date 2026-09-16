# Bronze Data Dictionary — DVF

Source: `files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz`
Field definitions sourced from DGFiP's official notice ("Notice descriptive des
fichiers Demande de valeurs foncières") and the Cerema Datafoncier documentation,
not inferred from the raw values. All columns are raw/unchanged in Bronze — no
transformation is applied at this layer.

Null rates measured on the Paris (department 75) 2024 file, 25 August 2026.

## Transaction identification

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `id_mutation` | string | Internal identifier grouping all rows belonging to the same transaction (a single sale can span several rows — e.g. a house sold with an outbuilding on a separate parcel). | 0.0% |
| `date_mutation` | date | Date the transaction was recorded (not necessarily the deed signature date). | 0.0% |
| `numero_disposition` | integer | Disposition number distinguishing multiple dispositions within the same deed. | 0.0% |
| `nature_mutation` | categorical | Legal nature of the transaction. Observed values: `Vente` (ordinary sale), `Vente en l'état futur d'achèvement` (VEFA — off-plan sale), `Vente de terrain à bâtir` (buildable-land sale), `Adjudication` (auction), `Expropriation`, `Échange` (exchange, e.g. family/inheritance swaps — this is the category a "sale" at an unusually low price often actually belongs to). | 0.0% |

## Value

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `valeur_fonciere` | float | Declared transaction value in euros, as recorded in the notarial deed. Not audited or corrected by DGFiP — extreme values can be genuine (VEFA staged payments, family transfers at symbolic prices) or data-entry errors. | 0.1% |

## Address

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `adresse_numero` | string | Street number. | 0.2% |
| `adresse_suffixe` | string | Street number suffix (bis, ter, etc.) — null whenever the address has no suffix, which is most of the time. | 95.8% |
| `adresse_nom_voie` | string | Street name. | 0.2% |
| `adresse_code_voie` | string | FANTOIR street code. | 0.2% |
| `code_postal` | string | Postal code (kept as string — leading zeros). | 0.2% |
| `code_commune` | string | INSEE municipality code (kept as string — leading zeros, and Corsica uses `2A`/`2B`). | 0.0% |
| `nom_commune` | string | Municipality name. | 0.0% |
| `code_departement` | string | INSEE department code. | 0.0% |
| `ancien_code_commune` | string | Pre-merger municipality code, populated only if the municipality has since merged with another (commune nouvelle) — none in Paris. | 100.0% |
| `ancien_nom_commune` | string | Pre-merger municipality name, same condition as above. | 100.0% |

## Cadastral reference

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `id_parcelle` | string | 14-character cadastral parcel reference: department code + municipality code + 3-digit prefix (non-zero only for former independent communes) + section letter(s) + plot number. This is the join key back to cadastral/parcel-level sources. | 0.0% |
| `ancien_id_parcelle` | string | Prior parcel reference, populated only if the parcel was resurveyed/resplit — none in this file. | 100.0% |
| `numero_volume` | string | Volume number, populated only for volume-based property division (rare, mostly complex commercial buildings). | 99.7% |

## Lots (co-ownership)

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `lot1_numero` | string | 1st co-ownership lot number involved in the transaction (apartments/condos are subdivided into numbered lots — e.g. the flat itself, a cellar, a parking space). | 11.8% |
| `lot2_numero` | string | 2nd lot number, if any. | 59.3% |
| `lot3_numero` | string | 3rd lot number, if any. | 93.3% |
| `lot4_numero` | string | 4th lot number, if any. | 97.6% |
| `lot5_numero` | string | 5th lot number, if any. | 99.0% |
| `lot1_surface_carrez` | float | Carrez-law surface (legal floor area for co-owned lots, distinct from `surface_reelle_bati`) for lot 1. | 62.8% |
| `lot2_surface_carrez` | float | Carrez surface for lot 2. | 88.8% |
| `lot3_surface_carrez` | float | Carrez surface for lot 3. | 99.0% |
| `lot4_surface_carrez` | float | Carrez surface for lot 4. | 99.8% |
| `lot5_surface_carrez` | float | Carrez surface for lot 5. | 99.9% |
| `nombre_lots` | integer | Total number of lots involved in the transaction. | 0.0% |

## Building characteristics

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `code_type_local` | categorical (int) | 1 = maison (house), 2 = appartement (apartment), 3 = dépendance isolée (standalone outbuilding — garage, cellar sold alone), 4 = local industriel et commercial ou assimilés. Null when the transaction is land-only. | 0.8% |
| `type_local` | categorical (string) | Text label matching `code_type_local`. | 0.8% |
| `surface_reelle_bati` | float | Real built surface: habitable + professional floor area. Does **not** include outbuilding/dependency surface. Null for nearly half of Paris rows — consistent with a large share of transactions being land-only, parking, or cellar-only sales rather than a data-quality problem. | 49.1% |
| `nombre_pieces_principales` | integer | Number of main rooms (bedrooms/living rooms — excludes kitchen, bathroom, hallway). | 0.8% |

## Land characteristics

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `code_nature_culture` | categorical | Cadastral land-use code for the parcel (e.g. sol, terre, jardin, bois) — describes the land itself, independent of any building on it. | 88.5% |
| `nature_culture` | categorical | Text label matching `code_nature_culture`. | 88.5% |
| `code_nature_culture_speciale` | categorical | Secondary/special land-use qualifier code, populated for specific categories (vineyards, orchards, etc.) — essentially absent in dense urban Paris. | 99.7% |
| `nature_culture_speciale` | categorical | Text label matching the above. | 99.7% |
| `surface_terrain` | float | Cadastral surface of the land parcel(s) transferred in the transaction. Distinct from `surface_reelle_bati` — this describes the land, not the building. Null for 88.5% of Paris rows: expected, since most Paris transactions are condo units in a building with no individually-attributed land parcel. | 88.5% |

## Geolocation

| Column | Type | Business definition | Null rate |
|---|---|---|---|
| `longitude` | float | Geocoded longitude of the parcel centroid, added by Etalab's geolocation process on top of the raw DGFiP extract. Null when geocoding failed. | 0.1% |
| `latitude` | float | Geocoded latitude, same process. | 0.1% |

## Sources

- DGFiP, *Notice descriptive des fichiers "Demande de valeurs foncières"* — https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20221017-153319/notice-descriptive-du-fichier-dvf-20221017.pdf
- Cerema, Documentation Datafoncier (DV3F) — https://doc-datafoncier.cerema.fr/doc/dv3f