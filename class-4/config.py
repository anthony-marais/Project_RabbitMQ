from dotenv import dotenv_values, load_dotenv

load_dotenv()

CONFIG = dotenv_values("class-4/.env")
print(CONFIG.values())