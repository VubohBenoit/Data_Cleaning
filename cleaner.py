########################################################
############## EPSI (2025): Data cleaning ##############
############## Version: 1.0              ###############
########################################################

import polars as pl
import logging
import re
from typing import Tuple

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("cleaning.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


class DataCleaner:
    def __init__(self, filepath: str):
        logger.info(f"Chargement du fichier {filepath}")
        self.df = pl.read_csv(filepath)

    def initial_summary(self):
        logger.info("🔍 Aperçu initial du dataset")
        logger.info(f"Nombre de lignes : {self.df.shape[0]}")
        logger.info(f"Noms de colonnes : {self.df.columns}")
        logger.info(f"Colonnes avec valeurs nulles :\n{self.df.null_count()}")

    def remove_duplicates(self):
        logger.info("🧹 Suppression des doublons")
        before = self.df.shape[0]
        self.df = self.df.unique()
        after = self.df.shape[0]
        logger.info(f"{before - after} doublons supprimés")

    def clean_names(self):
        logger.info("✍️ Nettoyage des noms (first_name, last_name)")
        self.df = self.df.with_columns(
            [
                pl.col("first_name")
                .str.strip_chars()
                .str.to_lowercase()
                .alias("first_name"),
                pl.col("last_name")
                .str.strip_chars()
                .str.to_lowercase()
                .alias("last_name"),
            ]
        )

    def clean_email(self):
        logger.info("📧 Validation des emails")
        before = self.df.shape[0]
        self.df = self.df.filter(
            pl.col("email").str.contains(r"^[\w\.-]+@[\w\.-]+\.\w+$")
        )
        after = self.df.shape[0]
        logger.info(f"{before - after} lignes avec emails invalides supprimées")

    def clean_birth_year(self, min_year: int = 1900, max_year: int = 2025):
        logger.info("📆 Nettoyage des années de naissance")
        self.df = self.df.with_columns([pl.col("birth_year").cast(pl.Int32)])
        before = self.df.shape[0]
        self.df = self.df.filter(
            (pl.col("birth_year") >= min_year) & (pl.col("birth_year") <= max_year)
        )
        after = self.df.shape[0]
        logger.info(f"{before - after} lignes avec birth_year invalide supprimées")

    def clean_income(self):
        logger.info("💰 Nettoyage des revenus (détection d’outliers)")

        # Forcer le cast explicite sans dupliquer
        self.df = self.df.with_columns(
            [pl.col("income").cast(pl.Float64).alias("income")]
        )

        # Calculer Q1 et Q3 avec alias différents pour éviter les conflits
        quantiles = self.df.select(
            [
                pl.col("income").quantile(0.25).alias("q1"),
                pl.col("income").quantile(0.75).alias("q3"),
            ]
        ).to_dict()

        q1 = quantiles["q1"][0]
        q3 = quantiles["q3"][0]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        before = self.df.shape[0]
        self.df = self.df.filter(
            (pl.col("income") >= lower_bound) & (pl.col("income") <= upper_bound)
        )
        after = self.df.shape[0]
        logger.info(f"{before - after} outliers supprimés selon IQR")

    def standardize_country_gender(self):
        logger.info("🌍 Standardisation des champs country et gender")
        self.df = self.df.with_columns(
            [
                pl.col("country").str.strip_chars().str.to_uppercase().alias("country"),
                pl.col("gender").str.strip_chars().str.to_lowercase().alias("gender"),
            ]
        )
        valid_genders = ["male", "female", "other"]
        before = self.df.shape[0]
        self.df = self.df.filter(pl.col("gender").is_in(valid_genders))
        after = self.df.shape[0]
        logger.info(f"{before - after} lignes avec gender invalide supprimées")

    def drop_nulls(self):
        logger.info("❌ Suppression des lignes avec données critiques manquantes")
        before = self.df.shape[0]
        self.df = self.df.drop_nulls(
            subset=[
                "id",
                "first_name",
                "last_name",
                "email",
                "birth_year",
                "country",
                "gender",
                "income",
            ]
        )
        after = self.df.shape[0]
        logger.info(f"{before - after} lignes supprimées (valeurs nulles)")

    def save(self, output_path: str = "cleaned_data.csv"):
        logger.info(f"💾 Sauvegarde du fichier nettoyé dans {output_path}")
        self.df.write_csv(output_path)
        logger.info("✅ Fichier sauvegardé avec succès")

    def run_cleaning_pipeline(self):
        logger.info("🚀 Début du pipeline de nettoyage")
        self.initial_summary()
        self.drop_nulls()
        self.remove_duplicates()
        self.clean_names()
        self.clean_email()
        self.clean_birth_year()
        self.clean_income()
        self.standardize_country_gender()
        self.save()
        logger.info("🏁 Pipeline terminé")
