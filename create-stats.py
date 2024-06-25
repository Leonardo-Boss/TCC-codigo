import sqlite3
import tqdm
import pickle
import pandas as pd
import matplotlib.pyplot as plt
def initdb():
    con = sqlite3.connect('meme-project.db')
    con.enable_load_extension(True)
    cur = con.cursor()
    cur.execute("SELECT load_extension('/home/eppi/Code/sqlite-hexhammdist/sqlite-hexhammdist.so', 'hexhammdist_init')");
    cur.close()
    return con

def search_hash(searching_hash, origin_path, hash_type, max_dist=9):
    _ = cur.execute("""SELECT path, hexhammdist(hash, ?) as ham, ? as compared
    FROM (SELECT hash, path FROM hashes WHERE hash_type = ?)
    WHERE ham <= ? ORDER BY ham ASC;""", (searching_hash, origin_path, hash_type, max_dist))
    return cur.fetchall()

def trash():

    con = initdb()
    cur = con.cursor()
    pd.set_option('display.max_columns', None)

    cur.execute("""SELECT hash, path, hash_type
        FROM hashes
        WHERE path like 'meme-project-raw-dataset/%' and hash_type != 'crop_resistant_hash'""")
    og = cur.fetchall()


    results = {}
    for i in tqdm.tqdm(og):
        results[i[1]] = search_hash(*i)


    cur.execute("""SELECT hash, path, hash_type FROM hashes WHERE path in 
        (SELECT path FROM memes
        WHERE (top IS NOT NULL OR bottom IS NOT NULL)
            AND angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL)
            AND hash_type != 'crop_resistant_hash';""")
    og_texts = cur.fetchall()
    len(og_texts)


    df = pd.read_sql_query("""SELECT og.hash_type, min(hexhammdist(og.hash, coh.hash)) AS ham_min, round(avg(hexhammdist(og.hash, coh.hash)), 2) as ham_avg, max(hexhammdist(og.hash, coh.hash)) as ham_max FROM hashes as og
        INNER JOIN memes AS co ON co.origin = og.path
        INNER JOIN hashes AS coh ON coh.path = co.path AND og.hash_type = coh.hash_type
        WHERE og.path like 'meme-project-raw-dataset/%'
            AND co.angle IS NOT NULL
            AND co.jpg_quality IS NOT NULL
            AND co.brightness IS NOT NULL
            AND co.contrast IS NOT NULL
            AND co.sharpness IS NOT NULL
            AND co.color IS NOT NULL
        GROUP BY og.hash_type""", con)
    df.to_csv('plots-hamm-dist/all-with-text.csv',index=False)

    df.head()
    ax = df.plot.bar(x='hash_type', stacked=False, title='angle without text', figsize=(10,5))
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/angle-without-text.png')

    """
    SELECT count(*) from memes as oc
    where co.top IS NOT NULL
        AND co.bottom IS NOT NULL
        AND co.angle IS NOT NULL
        AND co.jpg_quality IS NOT NULL
        AND co.brightness IS NOT NULL
        AND co.contrast IS NOT NULL
        AND co.sharpness IS NOT NULL
        AND co.color IS NOT NULL
            """


    con.commit()


    df=pd.read_sql_query("""SELECT ogh.hash_type, min(hexhammdist(ogh.hash, coh.hash)) AS ham_min, round(avg(hexhammdist(ogh.hash, coh.hash)), 2) as ham_avg, max(hexhammdist(ogh.hash, coh.hash)) as ham_max
    FROM memes as og
    INNER JOIN memes as co ON co.origin = og.path
    INNER JOIN hashes as ogh ON ogh.path = og.path
    INNER JOIN hashes as coh ON coh.path = co.path AND coh.hash_type = ogh.hash_type
    WHERE (og.top IS NOT NULL or og.bottom IS NOT NULL) AND
        (co.angle IS NOT NULL
        OR co.jpg_quality IS NOT NULL
        OR co.brightness IS NOT NULL
        OR co.contrast IS NOT NULL
        OR co.sharpness IS NOT NULL
        OR co.color IS NOT NULL)
    GROUP BY ogh.hash_type;""", con)

    df.to_csv('plots-hamm-dist/all-mixed-with-text.csv',index=False)

    df

    sb = pd.read_csv("plots-hamm-dist/same-base-different-text.csv")

    sb

    res = pd.concat([df['hash_type'], (df['ham_avg'] + (sb['ham_avg']-df['ham_avg']).mul(0.5)).round(2)], axis = 1)
    res


    a = tuple((t,h) for h,t in res.values)
    a

    cur.executemany('update ham_tresh set tresh = ? where hash_type = ?', a)

    con.commit()

    df.head()
    ax = df.plot.bar(x='hash_type', stacked=False, title='all mixed with text', figsize=(10,5))
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/all-mixed-with-text.png')


    df=pd.read_sql_query("""
        SELECT hash_type, min(ham) AS ham_min, round(avg(ham),2) AS ham_avg, max(ham) AS ham_max
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM hash_matches AS h
            INNER JOIN (SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                ON h.target_path = m.path
            ) AS h
            ON h.search_path = mo.path
            AND h.origin != mo.path
            AND h.super_origin = mo.origin
        GROUP BY h.hash_type;
        """, con)
    df.to_csv('plots-hamm-dist/same-base-different-text.csv',index=False)
    ax = df.plot.bar(x='hash_type', stacked=False, title='same base different text', figsize=(10,5))
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/same-base-different-text.png')


    cur.execute("""
        SELECT h.hash, h.path, h.hash_type, m.origin
        FROM hashes as h
        INNER JOIN memes as m ON h.path = m.path
        WHERE m.angle IS NULL
            AND m.jpg_quality IS NULL
            AND m.brightness IS NULL
            AND m.contrast IS NULL
            AND m.sharpness IS NULL
            AND m.color IS NULL;""")
    rows = cur.fetchall()

    for hash, path, htype, origin in tqdm.tqdm(rows):
        _ = cur.execute("""
            INSERT INTO hash_matches (search_hash, target_hash, search_path, target_path, hash_type, ham)
            SELECT ?, h.hash, ?, h.path, h.hash_type, hexhammdist(?, h.hash)
            FROM hashes AS h
            INNER JOIN memes as m ON h.path = m.path
            WHERE (m.angle IS NOT NULL
                OR m.jpg_quality IS NOT NULL
                OR m.brightness IS NOT NULL
                OR m.contrast IS NOT NULL
                OR m.sharpness IS NOT NULL
                OR m.color IS NOT NULL)
                AND m.origin != ?
                AND m.super_origin != ?
                AND h.hash_type = ?
            LIMIT 210;""", (hash, path, hash, path, origin, htype))

        con.commit()

        cur.close()

        con.close()

    diffbases = pd.read_sql_query("""
        SELECT h.hash_type, min(h.ham) AS ham_min, round(avg(h.ham),2) AS ham_avg, max(h.ham) AS ham_max
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM hash_matches AS h
            INNER JOIN (SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                ON h.target_path = m.path
            ) AS h
            ON h.search_path = mo.path
            AND h.origin != mo.path AND h.super_origin != mo.origin
        GROUP BY h.hash_type;""", con)

    ax = diffbases.plot.bar(x='hash_type', stacked=False, title='different bases', figsize=(10,5))
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/different-bases.png')

    diffbases.to_csv('plots-hamm-dist/different-bases.csv',index=False)


def get_confusion():
    con = initdb()
    pd.set_option('display.max_columns', None)

    fp_diff_base = pd.read_sql_query("""
        SELECT h.hash_type as hash_type, count(*) as false_positives_different_bases
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                INNER JOIN hash_matches AS h ON h.target_path = m.path
                ) AS h
            ON h.search_path = mo.path
            AND h.origin != mo.path
            AND h.super_origin != mo.origin
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        WHERE h.ham <= ht.tresh
        GROUP BY h.hash_type;""", con)
    fp_diff_base.to_csv('plots-hamm-dist/false-positives-different-bases.csv',index=False)
    total_negatives_diff_base = pd.read_sql_query("""
        SELECT h.hash_type as hash_type, count(*) as total_diff_base
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                INNER JOIN hash_matches AS h ON h.target_path = m.path
                ) AS h
            ON h.search_path = mo.path
            AND h.origin != mo.path
            AND h.super_origin != mo.origin
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        GROUP BY h.hash_type;""", con)
    fp_same_base = pd.read_sql_query("""
        SELECT h.hash_type AS hash_type, count(*) AS false_postives_same_base
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                INNER JOIN hash_matches AS h ON h.target_path = m.path
                ) AS h
            ON h.search_path = mo.path
            AND h.origin != mo.path
            AND h.super_origin = mo.origin
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        WHERE h.ham <= ht.tresh
        GROUP BY h.hash_type;""", con)
    total_negatives_same_base = pd.read_sql_query("""
        SELECT h.hash_type AS hash_type, count(*) AS total_false_positives_same_base
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                INNER JOIN hash_matches AS h ON h.target_path = m.path
                ) AS h
            ON h.search_path = mo.path
            AND h.origin != mo.path
            AND h.super_origin = mo.origin
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        GROUP BY h.hash_type;""", con)
    fp_same_base.to_csv('plots-hamm-dist/false-positives-same-bases.csv',index=False)
    tp = pd.read_sql_query("""
        SELECT h.hash_type, count(*) AS true_positives
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                INNER JOIN hash_matches AS h ON h.target_path = m.path
                ) AS h
            ON h.search_path = mo.path
            AND h.origin = mo.path
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        WHERE h.ham <= ht.tresh
        GROUP BY h.hash_type;""", con)
    tp.to_csv('plots-hamm-dist/true-positives.csv',index=False)
    total_positives = pd.read_sql_query("""
        SELECT h.hash_type, count(*) AS total_positives
        FROM (SELECT path, origin
            FROM memes
            WHERE angle IS NULL
            AND jpg_quality IS NULL
            AND brightness IS NULL
            AND contrast IS NULL
            AND sharpness IS NULL
            AND color IS NULL) AS mo
        INNER JOIN (
            SELECT h.search_path, h.target_path, h.ham, h.hash_type, m.origin, m.super_origin
            FROM (
                SELECT path, origin, super_origin
                FROM memes
                WHERE angle IS NOT NULL
                    OR jpg_quality IS NOT NULL
                    OR brightness IS NOT NULL
                    OR contrast IS NOT NULL
                    OR sharpness IS NOT NULL
                    OR color IS NOT NULL) AS m
                INNER JOIN hash_matches AS h ON h.target_path = m.path
                ) AS h
            ON h.search_path = mo.path
            AND h.origin = mo.path
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        GROUP BY h.hash_type;""", con)
    

    tpp = tp['true_positives']/total_positives['total_positives']
    tpp = tpp.mul(100)
    tpp = tpp.round(2)


    # fp_diff_base = pd.read_csv("plots-hamm-dist/false-positives-different-bases.csv")
    # fp_same_base = pd.read_csv("plots-hamm-dist/false-positives-same-bases.csv")
    # tp = pd.read_csv("plots-hamm-dist/true-positives.csv")

    fp_same_base = fp_same_base.drop(columns='hash_type')
    fp_diff_base = fp_diff_base.drop(columns='hash_type')

    fp_same_b = fp_same_base[fp_same_base.columns[0]]/total_negatives_same_base['total_false_positives_same_base']

    fp_same_b = fp_same_b.mul(100)
    fp_same_b = fp_same_b.round(2)

    fp_diff_b = fp_diff_base['false_positives_different_bases']/total_negatives_diff_base['total_diff_base']
    fp_diff_b = fp_diff_b.mul(100)
    fp_diff_b = fp_diff_b.round(3)


    abso = pd.concat([tp, fp_same_base, fp_diff_base], axis = 1)

    rel = pd.concat([tp['hash_type'], tpp, fp_same_b, fp_diff_b], axis=1)


    rel = rel.rename(columns={0:"true_positives", 1:"false_positives_same_base", 2:"false_postitives_different_base"})


    rel.to_csv("plots-hamm-dist/confusion-table-relative.csv", index=False)

    # abso=pd.read_csv("plots-hamm-dist/confusion-table-absolute.csv")


    ax = abso.plot.bar(x='hash_type', stacked=False, title='confusion bars absolute', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/confusion-bars-absolute.png')


    ax = rel.plot.bar(x='hash_type', stacked=False, title='confusion bars relative', figsize=(10,5), width=0.9)
    for container in ax.containers:
        ax.bar_label(container)
    plt.tight_layout()
    plt.savefig('plots-hamm-dist/confusion-bars-relative.png')


get_confusion()
