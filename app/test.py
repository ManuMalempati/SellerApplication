from .database import connect_database
import pandas as pd

# Connect to SQL Server
connection = connect_database()
cursor = connection.cursor()


# 6. Commit and close
connection.commit()
cursor.close()
connection.close()

print("Import complete!")
