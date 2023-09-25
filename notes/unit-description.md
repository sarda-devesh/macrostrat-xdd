
# Macrostrat stratigraphic concepts and database tables

Geologic units are contained in the `macrostrat.units` table.

They are Macrostrat's fundamental unit of description for a rock described for a particular place/region
(in a "stratigraphic column").


```sql
SET search_path TO  macrostrat, public;
```

## Unit context and names 

Geologic units are not confined to one location, but are often spatially expansive. They are often referred to by "stratigraphic names", 
semi-standardized proper nouns which identify rock units for geologists.

- This set of relationships is important to linking geologic units to the scientific literature.
- They often are referred to with predictable suffixes (e.g., *Omkyk **Member***, *Morrison **Formation***, *Tonto **Group***)
  which helps with discovery and extraction

The below query expresses the links between a 'geologic unit', our fundamental unit of
description of a rock seen in a place, and the 'stratigraphic names' that tend to
represent it in the literature (and their ages).

```sql
SELECT *
FROM strat_names s
-- The strat_names_meta (also referred to as "concepts") table allows synonyms
-- and associations to be tracked between named rock units
JOIN strat_names_meta c
  ON s.concept_id = c.concept_id
LEFT JOIN intervals i
  ON i.id = c.interval_id
  AND i.id != 0 -- Explicity set intervals for unknown units as null
-- Polymorhphic link between "units" and "strat names"
JOIN unit_strat_names us
  ON s.id = us.strat_name_id
JOIN units u
  ON u.id = us.unit_id
LIMIT 50;
```

## Unit descriptions

Each geologic unit contains one or several lithologies (`liths`; rock types)
which can each be described by a set of attributes (`lith_atts`).

- These are the first tokens we'd like to target for or fact sheet.
- Environments (`environs`) and economic uses (`econs`) are other descriptors of units that are similarly organized.

```sql
SELECT *
FROM units u
-- Rock types
-- Examples: sandstone, limestone, siltstone, granite, gneiss
-- Each unit can have multiple rock types
JOIN unit_liths ul
  ON u.id = ul.unit_id
JOIN liths l
  ON l.id = ul.lith_id
 -- Linking table between unit lithologies and their attributes
 -- (essentially, adjectives describing that component of the rock unit)
 -- Examples: silty mudstone, dolomitic sandstone, mottled shale, muscovite garnet granite
JOIN unit_lith_atts ula
  ON ula.unit_lith_id = ul.id
JOIN lith_atts la
  ON ula.lith_att_id = la.id
LIMIT 50
```