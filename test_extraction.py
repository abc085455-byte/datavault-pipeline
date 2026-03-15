from scripts.extract.api_extractor import run_extraction

print("=== DataVault Pipeline - Full Extraction Test ===\n")

uri = run_extraction()

print(f"\n=== DONE! ===")
print(f"Data S3 mein save hua: {uri}")