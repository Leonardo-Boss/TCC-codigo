import easyocr
import sqlite3
from tqdm import tqdm

con = sqlite3.connect("meme-project.db")
cur = con.cursor()

cur.execute("""
    SELECT path
    FROM memes
    WHERE top IS NOT NULL OR bottom IS NOT NULL
    """)
rows = cur.fetchall()

reader = easyocr.Reader(['en'])


cur.executemany("""
    INSERT INTO ocr (path, text) VALUES(?, ?)
    """, ((row[0],' '.join(reader.readtext(row[0], detail=0)))for row in tqdm(rows)))

con.commit()
