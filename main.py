########################################################
############## EPSI (2025): Data cleaning ##############
############## Version: 1.0              ###############
########################################################

from cleaner import DataCleaner

if __name__ == "__main__":
    print("🚀 Lancement du pipeline de nettoyage...")
    cleaner = DataCleaner("dataset_data_cleaning_5000_rows.csv")
    # cleaner = DataCleaner("cleaned_data.csv") # Tpour effectuer un text de verification
    cleaner.run_cleaning_pipeline()
    print("✅ Nettoyage terminé.")
