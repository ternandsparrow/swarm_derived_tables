CREATE TABLE species_level_functional_traits (
  species_name VARCHAR (255) PRIMARY KEY UNIQUE NOT NULL,
  family VARCHAR (50),
  genus VARCHAR (50),
  growth_form_summarised VARCHAR (50),
  substrate VARCHAR (50),
  max_plant_height decimal,
  plant_height_var decimal,
  leaf_area decimal,
  leaf_area_var decimal,
  seed_dry_mass decimal,
  seed_dry_mass_var decimal,
  leaf_mass_per_area decimal,
  leaf_mass_per_area_var decimal,
  sla decimal
)