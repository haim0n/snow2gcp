from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from snow2gcp.settings import SnowflakeAuth

def main():
    snowflake_auth = SnowflakeAuth()

    print(f"Snowflake User: {snowflake_auth.user}")
    print(f"Snowflake Account: {snowflake_auth.account}")

if __name__ == "__main__":
    main()
