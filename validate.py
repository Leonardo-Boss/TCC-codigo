import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

def cf_matrix(tn, fp, fn, tp, hash_type, name):
    for tni, fpi, fni, tpi, hash_typei in zip(tn.values, fp.values, fn.values, tp.values, hash_type.values):
        cf_matrix = np.array([[tni,fpi],[fni,tpi]])
        group_names = ["TN","FP","FN","TP"]
        group_counts = ["{0:0.0f}".format(value) for value in cf_matrix.flatten()]
        group_percentages = ["{0:.2%}".format(value) for value in cf_matrix.flatten()/np.sum(cf_matrix)]
        labels = [f"{v1}\n{v2}\n{v3}" for v1, v2, v3 in zip(group_names,group_counts,group_percentages)]
        labels = np.asarray(labels).reshape(2,2)
        x = sns.heatmap(cf_matrix, annot=labels, fmt="", cmap='Blues')
        x.set_title(f'{name}-{hash_typei}')
        x = x.get_figure()
        if not x: return
        x.savefig(f"plots-hamm-dist/{name}-{hash_typei}.png")
        plt.clf()

def calc_params(tn, fp, fn, tp, hash_type):
    accuracy = (tp+tn)/(tp+tn+fn+fp)
    recall = tp/(tp+fn)
    specificity = tn/(tn+fp)
    ppv = tp/(tp+fp)
    npv = tn/(tn+fn)
    table = pd.concat([hash_type, accuracy, recall, specificity, ppv, npv], axis = 1)
    return table.rename({0:'Accuracy',1:'Recall',2:'Specificity',3:'PPV',4:'NPV'}, axis = 1)

def get_img():
    TP_img = pd.read_sql_query("""
        SELECT h.hash_type, count(*) AS TP_img
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
    FN_img = pd.read_sql_query("""
        SELECT count(*) AS FN_img
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
        WHERE h.ham > ht.tresh
        GROUP BY h.hash_type;""", con)
    FP_img = pd.read_sql_query("""
        SELECT count(*) as FP_img
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
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        WHERE h.ham <= ht.tresh
        GROUP BY h.hash_type;""", con)
    TN_img = pd.read_sql_query("""
        SELECT count(*) as TN_img
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
        INNER JOIN ham_tresh AS ht ON ht.hash_type = h.hash_type
        WHERE h.ham > ht.tresh
        GROUP BY h.hash_type;""", con)

    cf_matrix(TN_img['TN_img'], FP_img['FP_img'], FN_img['FN_img'], TP_img['TP_img'], TP_img['hash_type'],'confusion-matrix-img')
    img = calc_params(TN_img['TN_img'], FP_img['FP_img'], FN_img['FN_img'], TP_img['TP_img'], TP_img['hash_type'])
    img.to_csv('validation-img.csv', index=False)
    return img

def get_txt():
    TP_txt = pd.read_sql_query("""
        SELECT h1.hash_type, count(*) as TP_txt
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
    TN_txt = pd.read_sql_query("""
        SELECT count(*) as TN_txt
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
    FN_txt = pd.read_sql_query("""
        SELECT count(*) as FN_txt
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
    FP_txt = pd.read_sql_query("""
        SELECT count(*) as FP_txt
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
    cf_matrix(TN_txt['TN_txt'], FP_txt['FP_txt'], FN_txt['FN_txt'], TP_txt['TP_txt'], TP_txt['hash_type'],'confusion-matrix-txt')
    txt = calc_params(TN_txt['TN_txt'], FP_txt['FP_txt'], FN_txt['FN_txt'], TP_txt['TP_txt'], TP_txt['hash_type'])
    txt.to_csv('validation-txt.csv', index=False)
    return txt

def get_txt_img():
    TP_txt_img = pd.read_sql_query("""
        SELECT hm.hash_type, count(*) AS TP_txt_img
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE hm.type='correct'
        AND hm.ham <= ht.tresh
        GROUP BY ht.hash_type;
                      """,con)
    FN_txt_img = pd.read_sql_query("""
        SELECT count(*) AS FN_txt_img
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE hm.type='correct'
        AND hm.ham > ht.tresh
        GROUP BY hm.hash_type;
                      """,con)

    FP_txt_img = pd.read_sql_query("""
        SELECT count(*) AS FP_txt_img
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE (hm.type='diff_base'
        OR hm.type='same_base_diff_text')
        AND hm.ham <= ht.tresh
        GROUP BY ht.hash_type;
                      """,con)
    TN_txt_img = pd.read_sql_query("""
        SELECT count(*) AS TN_txt_img
        FROM hash_matches_img as hm
        INNER JOIN ham_tresh as ht ON ht.hash_type = hm.hash_type
        WHERE (hm.type='diff_base'
        OR hm.type='same_base_diff_text')
        AND hm.ham > ht.tresh
        GROUP BY hm.hash_type;
                      """,con)

    cf_matrix(TN_txt_img['TN_txt_img'], FP_txt_img['FP_txt_img'], FN_txt_img['FN_txt_img'], TP_txt_img['TP_txt_img'], TP_txt_img['hash_type'],'confusion-matrix-txt_img')
    txt_img = calc_params(TN_txt_img['TN_txt_img'], FP_txt_img['FP_txt_img'], FN_txt_img['FN_txt_img'], TP_txt_img['TP_txt_img'], TP_txt_img['hash_type'])
    txt_img.to_csv('validation-txt_img.csv', index=False)
    return txt_img

con = sqlite3.connect("meme-project.db")
con.enable_load_extension(True)
cur = con.cursor()
cur.execute("SELECT load_extension('/home/eppi/Code/sqlite-hexhammdist/sqlite-hexhammdist.so', 'hexhammdist_init')");

img = img.round(2)

img.to_csv('validation-img.csv', index = False)

txt = txt.round(2)

txt.to_csv('validation-txt.csv', index = False)

txt_img = txt_img.round(2)

txt_img.to_csv('validation-txt_img.csv', index = False)

img = pd.read_csv('validation-img.csv')
txt = pd.read_csv('validation-txt.csv')
txt_img = pd.read_csv('validation-txt_img.csv')

table = pd.concat([img, txt, txt_img], axis=0)

table = table.round(2)

table.to_csv('validation.csv', index=False)

x = pd.concat([txt_img['hash_type'][:7], txt_img[:7].drop('hash_type',axis=1).subtract(img.drop('hash_type', axis=1))], axis = 1).round(2)

x

x.drop('hash_type', axis=1).mean()
