# GDPR Analysis — DVF Personal Data and Re-identification Risk

## What DVF actually contains

DVF is published as an anonymized extract: no party names, no notarial deed
details. But the Bronze data dictionary (RETL0-90) shows what remains per
transaction: a full street address (`adresse_numero`, `adresse_nom_voie`,
`code_postal`), a 14-character cadastral parcel reference (`id_parcelle`), an
exact transaction price (`valeur_fonciere`), and a date (`date_mutation`), plus
geolocated coordinates. Removing the name field does not remove the combination
of address + parcel + price + date — and that combination is the question this
analysis has to answer.

## Is this personal data under GDPR?

GDPR Article 4(1) defines personal data as information relating to a natural
person who is identifiable "directly or indirectly." Indirect identifiability is
assessed by whether a person can reasonably be singled out using available means
— it does not require a name field. An address is one of the strongest
quasi-identifiers there is, precisely because most addresses have exactly one
owner at a time. For a house (as opposed to one unit among many in an apartment
building), address + date is frequently sufficient on its own to single out a
specific transaction and, from there, a specific seller and buyer.

## The re-identification pathway is real, not hypothetical

DVF is not the only data source in play. France's Service de la Publicité
Foncière (SPF) is legally required to deliver ownership and transaction records
to any requester: CERFA form 11187 requests copies of registered property
documents, and CERFA form 11194 requests the legal situation of a specific
property, both processed within roughly ten working days, available to "toute
personne" (any person), not just interested parties. So a DVF row that pins down
an address, price, and date can be matched to its legal owner through a
low-cost, entirely legal public channel — it does not require a data breach or
a sophisticated attack, just a form and a short wait.

This is exactly the risk DGFiP and the CNIL themselves flag. In publishing DVF
as open data, DGFiP explicitly states that reuse "ne peut avoir ni pour objet ni
pour effet" (can have neither the purpose nor the effect) of permitting the
re-identification of the people involved, and DVF-derived material must not be
indexed by search engines. The CNIL's own guidance on open data notes that
increasing digital cross-referencing capability raises re-identification risk
over time, and that a diffuser's legitimate interest in principle requires
pseudonymization measures. DGFiP and the CNIL are, in effect, already treating
DVF as re-identification-sensitive data, despite it containing no name field —
which settles the question above in practice, not just in theory.

## What Licence Ouverte 2.0 permits and requires

Separately from GDPR, DVF is published under Licence Ouverte 2.0, a permissive
license: free reuse, adaptation, redistribution, and commercial use, worldwide,
for unlimited duration. The only obligation is attribution — citing the source
(DGFiP/Etalab) and the date of last update of the data reused. The license says
nothing about re-identification; that constraint comes from DGFiP's publication
terms and GDPR, not from the license itself, and is a separate obligation that
using the data under Licence Ouverte 2.0 does not waive.

## Conclusion

DVF should be treated as personal data for GDPR purposes at the individual
transaction level, particularly for houses and other single-owner properties,
because address + parcel + price + date is a quasi-identifier combination that
can be linked to an identified owner through a legitimate, low-barrier public
channel (SPF/CERFA 11187, 11194) — a risk DGFiP and the CNIL already recognize
explicitly in DVF's own publication terms.

For this project, this has one concrete design consequence, already reflected
in the architecture: no full-resolution, address-level DVF record should be
exposed through any public-facing output. Gold is aggregated to municipality
and quarter grain specifically because that grain cannot be traced back to an
individual transaction or owner — this is a privacy-by-design property of the
architecture, not only an analytical choice. The Bronze and Silver layers,
which do retain address-level and parcel-level detail, must stay internal to
the pipeline and never be published, indexed, or exposed through Metabase.
Attribution to DGFiP/data.gouv.fr, with the date of last update, must appear
wherever DVF-derived material is published, per Licence Ouverte 2.0.

## Sources

- DGFiP / data.gouv.fr, dataset terms for *Demandes de valeurs foncières géolocalisées*
- CNIL, *Recommandations aux diffuseurs de données ouvertes (open data)* — https://www.cnil.fr/sites/cnil/files/2024-06/recommandations_diffuseurs_de_donnees_ouvertes_open_data.pdf
- Licence Ouverte 2.0 (Etalab) — https://www.etalab.gouv.fr/wp-content/uploads/2017/04/ETALAB-Licence-Ouverte-v2.0.pdf
- Décret n°55-22 du 4 janvier 1955 and CERFA 11187/11194 — Service de la Publicité Foncière public-access procedure
