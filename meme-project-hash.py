import sqlite3
from PIL import Image
import imagehash
from os import scandir
from tqdm import tqdm


def initdb():
    con = sqlite3.connect('test.db')
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hashes(
        path TEXT,
        hash_type TEXT,
        hash TEXT,
        PRIMARY KEY (path, hash_type)
    );
    """)
    con.commit()
    cur.close()
    return con

HASHES = [
    # imagehash.ImageMultiHash,
    imagehash.average_hash,
    imagehash.colorhash,
    imagehash.crop_resistant_hash,
    imagehash.dhash,
    imagehash.dhash_vertical,
    imagehash.phash,
    imagehash.phash_simple,
    imagehash.whash
]

def hash(im, path, con, hash_function):
    cur = con.cursor()
    result_hash = str(hash_function(im))
    cur.execute(f"INSERT INTO hashes (path, hash_type, hash) VALUES (?, ?, ?);", (path, hash_function.__name__, result_hash,))
    con.commit()
    cur.close()

def pipeline(con, path,):
    image = Image.open(path)
    for h in HASHES:
        hash(image, path, con, h)

if '__main__' == __name__:
    con = initdb()

    og = list(map(lambda x: x.path, filter(lambda x: 'jpg' in x.name,scandir('meme-project-raw-dataset/'))))
    mod = list(map(lambda x: x.path, scandir('meme-project-pipeline-result/')))
    files = og + mod

    for file in tqdm(files):
        pipeline(con, file)
