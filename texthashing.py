import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import sqlite3
from tqdm import tqdm
con = sqlite3.connect('meme-project.db')
con.enable_load_extension(True)
cur = con.cursor()
cur.execute("SELECT load_extension('/home/eppi/Code/sqlite-hexhammdist/sqlite-hexhammdist.so', 'hexhammdist_init')");
pd.set_option('display.max_columns', None)

class SimHash:
    def __init__(self, text, letters_per_shingle=2):
        text = text.lower()
        text = ''.join(filter(self.isalpha, text))
        shingles = self.shingle(text, letters_per_shingle)
        hashes = np.array([np.array(self.get_pure_binary(hash(shingle))) for shingle in shingles])
        self.hash = np.round(sum(hashes.astype('float'))/len(hashes)).astype('bool')

    def shingle(self, text:str, letters):
        text = text.ljust(letters, ' ')
        return (tuple(text[i:i+letters]) for i in range(len(text)-letters+1))

    def isalpha(self, t:str):
        return t in "abcdefghijklmnopqrstuvwxyz "

    def get_pure_binary(self, num):
        bin = format(num, 'b')
        if bin[0] == '-':
            bin = '1' + bin[1:]
        if len(bin) < 64:
            bin = bin.zfill(64)
        return np.array(list(bin))

    def __sub__(self, other):
        return np.count_nonzero(self.hash!=other.hash)

    def __str__(self):
        return self._binary_array_to_hex(self.hash.flatten())

    def _binary_array_to_hex(self, arr):
        """
        internal function to make a hex string out of a binary array.
        """
        bit_string = ''.join(str(b) for b in 1 * arr.flatten())
        width = int(np.ceil(len(bit_string) / 4))
        return '{:0>{width}x}'.format(int(bit_string, 2), width=width)

class SimHashWS(SimHash):
    def isalpha(self, t:str):
        return True

class SimHashIM(SimHash):
    def __init__(self, text, image_hash, image_weight=30, letters_per_shingle=2):
        text = text.lower()
        text = ''.join(filter(self.isalpha, text))
        shingles = self.shingle(text, letters_per_shingle)
        hashes = np.array([np.array(self.get_pure_binary(hash(shingle))) for shingle in shingles])
        image_hash = self.hex_to_flathash(image_hash)
        weight = int(len(hashes)*(image_weight/100)) # in percentage
        weight = weight if weight else 1
        try:
            hashes = np.append(hashes, np.array([image_hash]*weight), axis=0)
        except Exception as e:
            print(e)
            print(image_hash.shape)
            print(np.array([image_hash]*weight).shape)
            exit()
        self.hash = np.round(sum(hashes.astype('float'))/len(hashes)).astype('bool')

    def hex_to_flathash(self, hexstr):
        binary_array = '{:0>64b}'.format(int(hexstr, 16))
        return np.array([int(d) for d in binary_array])

class SimHashWSIM(SimHashIM):
    def isalpha(self, t:str):
        return True

def hex_to_flathash(hexstr):
    binary_array = '{:0>64b}'.format(int(hexstr, 16))
    return np.array([int(d) for d in binary_array])


def shingle(text:str, letters):
    text = text.ljust(letters, ' ')
    return (tuple(text[i:i+letters]) for i in range(len(text)-letters+1))

def isalpha(t:str):
    return t in "abcdefghijklmnopqrstuvwxyz "

def get_pure_binary(num):
    bin = format(num, 'b')
    if bin[0] == '-':
        bin = '1' + bin[1:]
    if len(bin) < 64:
        bin = bin.zfill(64)
    return np.array(list(bin))

def dum():


    image_weight=30
    letters_per_shingle=2
    text = "hello wolrd"
    image_hash = '2ca1c330ff'

    SimHashIM(text, image_hash).hash.shape

    text = text.lower()
    text = ''.join(filter(isalpha, text))
    shingles = shingle(text, letters_per_shingle)
    hashes = np.array([np.array(get_pure_binary(hash(shingle))) for shingle in shingles])
    hashes.shape

    image_hash = hex_to_flathash(image_hash)
    weight = int(len(hashes)*(image_weight/100)) # in percentage
    np.array([image_hash]*weight).shape

    hashes = np.append(hashes, np.array([image_hash]*weight), axis=0)
    h = np.round(sum(hashes.astype('float'))/len(hashes)).astype('bool')
    h


def hash_text():

    cur.execute("""
                SELECT text, path
                FROM ocr;
                """)
    rows = cur.fetchall()
    len(rows)

    stuff = ((path, text, 'SimHashWS', str(SimHashWS(text))) for text, path in tqdm(rows))
    cur.executemany("""
                INSERT INTO text_hashes (path, text, hash_type, hash)
                VALUES (?, ?, ?, ?)
                """, stuff)

    con.commit()


def hash_textim():

    cur.execute("""
                SELECT ocr.text, ocr.path, h.hash, h.hash_type
                FROM ocr
                INNER JOIN hashes as h ON h.path = ocr.path;
                """)
    rows = cur.fetchall()
    len(rows)

    stuff = ((path, f'SimHashIM+{hash_type}', str(SimHashIM(text,image_hash))) for text, path, image_hash, hash_type in tqdm(rows))
    cur.executemany("""
                INSERT INTO text_image_hashes (path, hash_type, hash)
                VALUES (?, ?, ?)
                """, stuff)

    stuff = ((path, f'SimHashWSIM+{hash_type}', str(SimHashWSIM(text, image_hash))) for text, path, image_hash, hash_type in tqdm(rows))
    cur.executemany("""
                INSERT INTO text_image_hashes (path, hash_type, hash)
                VALUES (?, ?, ?)
                """, stuff)

    con.commit()


def stats():

    diff = pd.read_sql_query("""
        SELECT h1.hash_type, min(hexhammdist(h1.hash, h2.hash)) AS min, round(avg(hexhammdist(h1.hash, h2.hash)),2) AS avg, max(hexhammdist(h1.hash, h2.hash)) AS max
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                LIMIT 20) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin != h1.path
            AND h2.hash_type = h1.hash_type
        GROUP BY h1.hash_type;
                      """,con)
    ax = diff.plot.bar(x='hash_type', stacked=False, title='different text', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/different-text.png')

    correct = pd.read_sql_query("""
        SELECT h1.hash_type, min(hexhammdist(h1.hash, h2.hash)) AS min, round(avg(hexhammdist(h1.hash, h2.hash)),2) AS avg, max(hexhammdist(h1.hash, h2.hash)) AS max
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                LIMIT 20) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin = h1.path
            AND h2.hash_type = h1.hash_type
        GROUP BY h1.hash_type;
                      """,con)
    ax = correct.plot.bar(x='hash_type', stacked=False, title='correct text', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/correct-text.png')

def conf_matrix():
    fp = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as false_positives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                LIMIT 20) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin != h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) <= ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    fp = fp['false_positives']

    tp = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as true_positives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                ) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin = h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) <= ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    tp =tp['true_positives']

    tn = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as true_negatives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                LIMIT 20) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin != h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) > ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    tn = tn['true_negatives']

    fn = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as false_negatives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                ) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin = h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) > ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    fn = fn['false_negatives']


    cf_matrix = np.array([[tn[0],fp[0]],[fn[0],tp[0]]])
    group_names = ["Verdadeiros Negativos","Falsos Positivos","Falsos Negativos","Verdadeiros Positivos"]
    group_counts = ["{0:0.0f}".format(value) for value in cf_matrix.flatten()]
    group_percentages = ["{0:.2%}".format(value) for value in cf_matrix.flatten()/np.sum(cf_matrix)]
    labels = [f"{v1}\n{v2}\n{v3}" for v1, v2, v3 in zip(group_names,group_counts,group_percentages)]
    labels = np.asarray(labels).reshape(2,2)
    x = sns.heatmap(cf_matrix, annot=labels, fmt="", cmap='Blues')
    x.set_title("SimHash")
    x = x.get_figure()
    if not x: return
    x.savefig("plots-hamm-dist/confusion-matrix-SimHash.png")

def conf_matrixWS():

    fp = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as false_positives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                LIMIT 20) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin != h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) <= ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    fp = fp['false_positives']

    tp = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as true_positives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                ) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin = h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) <= ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    # tp =tp['true_positives']
    tn = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as true_negatives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                LIMIT 20) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin != h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) > ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    tn = tn['true_negatives']
    fn = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as false_negatives
        FROM (
            SELECT h.path, h.hash, m.origin, h.hash_type, h.text
            FROM (SELECT path, origin FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL)
                    AND angle IS NULL
                    AND jpg_quality IS NULL
                    AND brightness IS NULL
                    AND contrast IS NULL
                    AND sharpness IS NULL
                    AND color IS NULL) AS m
            INNER JOIN text_hashes AS h ON h.path = m.path
            ) AS h1
        INNER JOIN (
            SELECT h.path, h.hash, m.origin, m.super_origin, h.text, h.hash_type
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
                    (angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL)
                ) AS m
                INNER JOIN text_hashes AS h ON h.path = m.path
                ) AS h2
            ON h2.origin = h1.path
            AND h2.hash_type = h1.hash_type
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h1.hash_type
        WHERE hexhammdist(h1.hash, h2.hash) > ht.tresh
        GROUP BY h1.hash_type;
                      """,con)
    fn = fn['false_negatives']


    # cf_matrix = np.array([[tn[1],fp[1]],[fn[1],tp[1]]])
    # group_names = ["Verdadeiros Positivos","Falsos Positivos","Falsos Negativos","Verdadeiros Positivos"]
    # group_counts = ["{0:0.0f}".format(value) for value in cf_matrix.flatten()]
    # group_percentages = ["{0:.2%}".format(value) for value in cf_matrix.flatten()/np.sum(cf_matrix)]
    # labels = [f"{v1}\n{v2}\n{v3}" for v1, v2, v3 in zip(group_names,group_counts,group_percentages)]
    # labels = np.asarray(labels).reshape(2,2)
    # x = sns.heatmap(cf_matrix, annot=labels, fmt="", cmap='Blues')
    # x.set_title("SimHashWS")
    # x = x.get_figure()
    # if not x: return
    # x.savefig("plots-hamm-dist/confusion-matrix-SimHashWS.png")

    abso = pd.concat([tp['hash_type'],
                     tp['true_positives'],
                     fp], axis=1)
    abso = abso.rename({'true_positives':"Verdadeiros Positivos", 'false_positives':"Falsos Positivos"},axis=1)
    abso
    ax = abso.plot.bar(x='hash_type', stacked=False, title='confusion bars text hash absolute', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/confusion-bars-texthash-absolute.png')

    rel = pd.concat([tp['hash_type'],
                     (tp['true_positives']/(tp['true_positives']+fn)).mul(100).round(2),
                     (fp/(fp+fn)).mul(100).round(2)], axis=1)
    rel = rel.rename({0:"Verdadeiros Positivos", 1:"Falsos Positivos"},axis=1)
    rel
    ax = rel.plot.bar(x='hash_type', stacked=False, title='confusion bars text hash relative', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/confusion-bars-texthash-relative.png')


def stats_textim():


    fp_sb = pd.read_sql_query("""
        SELECT count(*) AS false_positives_same_base
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE hm.type='same_base_diff_text'
        AND hm.ham <= ht.tresh
        GROUP BY ht.hash_type;
                      """,con)
    tn_sb = pd.read_sql_query("""
        SELECT count(*) AS true_negatives_same_base
        FROM hash_matches_img as hm
        WHERE hm.type='same_base_diff_text'
        GROUP BY hm.hash_type;
                      """,con)

    # ax = diff_t_same_b.plot.bar(x='hash_type', stacked=False, title='text-image-diff-text-same-base', figsize=(10,5), width=0.9)
    # for container in ax.containers:
    #     ax.bar_label(container)
    # plt.tight_layout()
    # plt.savefig('plots-hamm-dist/text-image-diff-text-same-base.png')

    tp = pd.read_sql_query("""
        SELECT hm.hash_type, count(*) AS true_positives
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE hm.type='correct'
        AND hm.ham <= ht.tresh
        GROUP BY ht.hash_type;
                      """,con)
    tt = pd.read_sql_query("""
        SELECT count(*) AS total_true
        FROM hash_matches_img as hm
        WHERE hm.type='correct'
        GROUP BY hm.hash_type;
                      """,con)
    fn = pd.read_sql_query("""
        SELECT count(*) AS false_negatives
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE hm.type='correct'
        AND hm.ham > ht.tresh
        GROUP BY hm.hash_type;
                      """,con)

    # ax = correct.plot.bar(x='hash_type', stacked=False, title='text-image-correct', figsize=(10,5), width=0.9)
    # for container in ax.containers:
    #     ax.bar_label(container)
    # plt.tight_layout()
    # plt.savefig('plots-hamm-dist/text-image-correct.png')

    fp_db = pd.read_sql_query("""
        SELECT count(*) AS false_positives_diff_base
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE hm.type='diff_base'
        AND hm.ham <= ht.tresh
        GROUP BY ht.hash_type;
                      """,con)

    tn_db = pd.read_sql_query("""
        SELECT count(*) AS true_negatives_diff_base
        FROM hash_matches_img as hm
        WHERE hm.type='diff_base'
        GROUP BY hm.hash_type;
                      """,con)

    # ax = diff_base.plot.bar(x='hash_type', stacked=False, title='text-image-different-base', figsize=(10,5), width=0.9)
    # for container in ax.containers:
    #     ax.bar_label(container)
    # plt.tight_layout()
    # plt.savefig('plots-hamm-dist/text-image-different-base.png')

    df = pd.concat([correct, diff_t_same_b], axis=1)

    r = df.apply(lambda x:(x[0],round(x[1]+ (x[2]-x[1])/2)), axis=1)
    r

    rows = tuple(r)

    cur.executemany('INSERT INTO ham_tresh (hash_type, tresh) VALUES (?, ?)', rows)

    con.commit()

    tp

    abso = pd.concat([tp, fp_sb, fp_db], axis=1)

    fp_db['false_positives_diff_base']/tn_db['true_negatives_diff_base']

    rel = pd.concat([tp['hash_type'],
                     (tp['true_positives']/tt['total_true']).mul(100).round(2),
                     (fp_sb['false_positives_same_base']/tn_sb['true_negatives_same_base']).mul(100).round(2),
                     (fp_db['false_positives_diff_base']/tn_db['true_negatives_diff_base']).mul(100).round(2)], axis=1)

    rel = rel.rename({0:"true positives", 1:"false_positives_same_base", 2:"false_positives_different_base"}, axis=1)

    abso.to_csv('plots-hamm-dist/confusion-bars-textimghash-absolute.csv', index=False)
    rel.to_csv('plots-hamm-dist/confusion-bars-textimghash-relative.csv', index=False)

    rel

    ax = abso.plot.bar(x='hash_type', stacked=False, title='confusion bars textimg hash absolute', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/confusion-bars-textimghash-absolute.png')

    ax = rel.plot.bar(x='hash_type', stacked=False, title='confusion bars textimg hash relative', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/confusion-bars-textimghash-relative.png')

conf_matrixWS()
