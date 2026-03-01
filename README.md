# Financial-Statement-Parsers
Python scripts to parse PDF financial statements, insert into a postgres database and populate a power bi report. 

##Features
Parses the statement date, transactions, and aggregates the total debit and credits. 
Both bank and credit parser insert into their own table in a postgres database. 
Twe separate reports created in Power BI. 

##Requirements
Package            Version
------------------ -----------
cffi               2.0.0
charset-normalizer 3.4.4
cryptography       46.0.5
greenlet           3.3.2
numpy              2.4.2
pandas             3.0.1
pdfminer.six       20251230
pdfplumber         0.11.9
pillow             12.1.1
pip                26.0.1
psycopg2           2.9.11
pycparser          3.0
pypdfium2          5.5.0
python-dateutil    2.9.0.post0
six                1.17.0
SQLAlchemy         2.0.47
typing_extensions  4.15.0
tzdata             2025.3
