from sqlalchemy import create_engine

class Connection:
    def __init__(self):
        self.hostname = "localhost"
        self.port = 5432
        self.database = "finance"
        self.username = "postgres"
        self.password = "Thischangeseverything335"
        
    def postgres_connect(self):
        try:
            connection = create_engine(
                f"postgresql://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database}",
                pool_pre_ping=True
            )
            print("Connection to PostgreSQL database successful!")
            return connection
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            return None