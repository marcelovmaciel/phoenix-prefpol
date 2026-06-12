# The form of dissent 

This monorepo contains modular Julia packages and manuscript material for the
Brazil/ESEB preference-polarization replication.

| Path | Role |
|---|---|
| `PreferenceProfiles/` | Reusable formal preference and social-choice package. |
| `PrefPol/` | Applied Brazil/ESEB replication package built on `PreferenceProfiles`. |
| `PreferencePlots/` | Plotting companion package. |
| `VotingGeometry/` | Saari-style voting geometry, decomposition, scoring, and plotting package. |
| `writing/` | Manuscript-related material and generated paper assets. |

For the publication-facing replication workflow, start with
`PrefPol/README.md` and `PrefPol/config/publication.toml`.
