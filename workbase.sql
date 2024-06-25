SELECT load_extension('/home/eppi/Code/sqlite-hexhammdist/sqlite-hexhammdist.so', 'hexhammdist_init');

INSERT INTO hash_matches (search_hash, target_hash, search_path, target_path, hash_type, ham)
SELECT h.hash, mh.hash, h.path, mh.path, h.hash_type, hexhammdist(h.hash, mh.hash) as ham
FROM memes AS m
INNER JOIN hashes AS h ON h.path = m.path
INNER JOIN (
        SELECT h.hash_type as hash_type, h.hash AS hash, m1.origin AS origin, m.origin AS origin1, m.path as path
        FROM memes AS m
        INNER JOIN hashes AS h ON h.path = m.path
        INNER JOIN memes AS m1 ON m1.path = m.origin) AS mh
        ON mh.origin = m.origin AND h.hash_type = mh.hash_type
WHERE m.angle IS NULL
AND m.jpg_quality IS NULL
AND m.brightness IS NULL
AND m.contrast IS NULL
AND m.sharpness IS NULL
AND m.color IS NULL;

CREATE TABLE hash_matches (
        search_hash TEXT,
        target_hash TEXT,
        search_path TEXT,
        target_path TEXT,
        hash_type TEXT,
        ham INTEGER,
        PRIMARY KEY (search_path, target_path, hash_type));

SELECT hash_type, min(ham), round(avg(ham),2), max(ham)

SELECT COUNT(*)
FROM hash_matches
LEFT JOIN memes ON memes.origin = hash_matches.search_path AND memes.path = hash_matches.target_path
WHERE memes.origin IS NULL AND (memes.top IS NULL AND memes.bottom IS NULL)
GROUP BY hash_type;

SELECT ogh.hash_type, min(hexhammdist(ogh.hash, coh.hash)) AS ham_min, round(avg(hexhammdist(ogh.hash, coh.hash)), 2) as ham_avg, max(hexhammdist(ogh.hash, coh.hash)) as ham_max

SELECT COUNT(*)
    FROM memes as og
    INNER JOIN memes as co ON co.origin=og.path
    INNER JOIN hashes as ogh ON ogh.path = og.path
    INNER JOIN hashes as coh ON coh.path = co.path AND coh.hash_type = ogh.hash_type
    WHERE (og.top IS NOT NULL or og.bottom IS NOT NULL)
        AND co.angle IS NOT NULL
        AND co.jpg_quality IS NOT NULL
        AND co.brightness IS NOT NULL
        AND co.contrast IS NOT NULL
        AND co.sharpness IS NOT NULL
        AND co.color IS NOT NULL
    GROUP BY ogh.hash_type;

SELECT COUNT(*)
FROM hashes AS og
INNER JOIN memes AS co ON co.origin = og.path
INNER JOIN hashes AS coh ON coh.path = co.path AND og.hash_type = coh.hash_type
WHERE og.path LIKE 'meme-project-raw-dataset/%'
    AND co.angle IS NOT NULL
    AND co.jpg_quality IS NOT NULL
    AND co.brightness IS NOT NULL
    AND co.contrast IS NOT NULL
    AND co.sharpness IS NOT NULL
    AND co.color IS NOT NULL
GROUP BY og.hash_type;

INSERT INTO hash_matches (search_hash, target_hash, search_path, target_path, hash_type, ham)
SELECT h1.hash, h2.hash, h1.path, h2.path, h1.hash_type, hexhammdist(h1.hash, h2.hash)
FROM (SELECT h.hash, h.path, h.hash_type, m.origin
    FROM hashes as h
    INNER JOIN memes as m ON h.path = m.path
    WHERE m.angle IS NULL
        AND m.jpg_quality IS NULL
        AND m.brightness IS NULL
        AND m.contrast IS NULL
        AND m.sharpness IS NULL
        AND m.color IS NULL) AS h1
INNER JOIN (SELECT h.hash, h.path, h.hash_type, m.origin, m.super_origin
    FROM hashes AS h
    INNER JOIN memes as m ON h.path = m.path
    WHERE m.angle IS NOT NULL
        OR m.jpg_quality IS NOT NULL
        OR m.brightness IS NOT NULL
        OR m.contrast IS NOT NULL
        OR m.sharpness IS NOT NULL
        OR m.color IS NOT NULL
    LIMIT 1001) AS h2
    ON h1.path != h2.origin
    AND h1.origin != h2.super_origin
    AND h1.hash_type = h2.hash_type;

-- select hashes og images
SELECT COUNT(*)
FROM hashes as h
INNER JOIN memes as m ON h.path = m.path
WHERE m.angle IS NULL
AND m.jpg_quality IS NULL
AND m.brightness IS NULL
AND m.contrast IS NULL
AND m.sharpness IS NULL
AND m.color IS NULL
group by h.hash_type;

SELECT count(*) FROM (
SELECT h.hash_type
FROM hashes as h
INNER JOIN memes as m ON h.path = m.path
WHERE m.angle IS NOT NULL
    OR m.jpg_quality IS NOT NULL
    OR m.brightness IS NOT NULL
    OR m.contrast IS NOT NULL
    OR m.sharpness IS NOT NULL
    OR m.color IS NOT NULL
LIMIT 210)
GROUP BY hash_type;

-- select hashes of modified images
SELECT count(*)
FROM hashes AS h
INNER JOIN memes as m ON h.path = m.path
WHERE m.angle IS NOT NULL
    OR m.jpg_quality IS NOT NULL
    OR m.brightness IS NOT NULL
    OR m.contrast IS NOT NULL
    OR m.sharpness IS NOT NULL
    OR m.color IS NOT NULL
GROUP BY h.hash_type;


-- select hashes of modified images
SELECT count(*)
FROM hash_matches as hm
INNER JOIN memes as m ON hm.target_path = m.path
WHERE m.angle IS NOT NULL
    OR m.jpg_quality IS NOT NULL
    OR m.brightness IS NOT NULL
    OR m.contrast IS NOT NULL
    OR m.sharpness IS NOT NULL
    OR m.color IS NOT NULL;

CREATE TABLE ocr(
    path TEXT PRIMARY KEY,
    text TEXT
);

-- get ham stats on images with different bases
SELECT h.hash_type, min(h.ham) AS ham_min, round(avg(h.ham),2) AS ham_avg, max(h.ham) AS ham_max

-- select same bases
SELECT count(*)
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
    AND h.origin != mo.path AND h.super_origin = mo.origin
GROUP BY h.hash_type;


SELECT COUNT(*)
FROM hash_matches AS h
INNER JOIN (SELECT path, origin, super_origin
    FROM memes
    WHERE angle IS NOT NULL
        OR jpg_quality IS NOT NULL
        OR brightness IS NOT NULL
        OR contrast IS NOT NULL
        OR sharpness IS NOT NULL
        OR color IS NOT NULL) AS m ON h.target_path = m.path;

(SELECT path, origin, super_origin
FROM memes
WHERE angle IS NOT NULL
    OR jpg_quality IS NOT NULL
    OR brightness IS NOT NULL
    OR contrast IS NOT NULL
    OR sharpness IS NOT NULL
    OR color IS NOT NULL)


-- ham tresholds

CREATE TABLE ham_tresh (
    hash_type TEXT PRIMARY KEY,
    tresh INTEGER
);

SELECT DISTINCT hash_type FROM hashes;

INSERT INTO ham_tresh (hash_type, tresh) VALUES ('average_hash', 6),
('colorhash', 5),
('dhash', 9),
('dhash_vertical', 10),
('phash', 10),
('phash_simple', 13),
('whash', 7);

SELECT * FROM ham_tresh;

-- select wrong ones different bases
SELECT h.hash_type, count(*)
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
GROUP BY h.hash_type;

-- same base wrong ones
SELECT h.hash_type, count(*)
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
GROUP BY h.hash_type;

-- correct ones
SELECT h.hash_type, count(*)
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
GROUP BY h.hash_type;


-- text hashing
CREATE TABLE IF NOT EXISTS text_hashes (
    path TEXT,
    text TEXT,
    hash_type TEXT,
    hash TEXT,
    PRIMARY KEY (path, hash_type)
);

CREATE TABLE IF NOT EXISTS text_hash_matches (
    search_path TEXT,
    target_path TEXT,
    search_hash TEXT,
    target_hash TEXT,
    hash_type TEXT,
    ham INTEGER,
    PRIMARY KEY (search_path, target_path, hash_type)
);

INSERT INTO text_hash_matches (search_path, target_path, search_hash, target_hash, hash_type, ham)
    SELECT h1.path, h2.path, h1.hash, h2.hash, h1.hash_type, hexhammdist(h1.hash, h2.hash) AS ham

SELECT h1.hash_type, avg(hexhammdist(h1.hash, h2.hash)) AS avg, max(hexhammdist(h1.hash, h2.hash)) AS max, count(*) as count
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
    AND h2.super_origin != h1.origin
    AND h2.hash_type = h1.hash_type
GROUP BY h1.hash_type;


SELECT * FROM text_hash_matches;


SELECT count(*)
FROM (SELECT path, origin FROM memes
    WHERE (top IS NOT NULL OR bottom IS NOT NULL)
        AND angle IS NULL
        AND jpg_quality IS NULL
        AND brightness IS NULL
        AND contrast IS NULL
        AND sharpness IS NULL
        AND color IS NULL) AS m
INNER JOIN text_hashes AS h ON h.path = m.path
GROUP BY hash_type;

SELECT count(*)
FROM (SELECT path, origin FROM memes
    WHERE (top IS NOT NULL OR bottom IS NOT NULL) AND
        (angle IS NOT NULL
        OR jpg_quality IS NOT NULL
        OR brightness IS NOT NULL
        OR contrast IS NOT NULL
        OR sharpness IS NOT NULL
        OR color IS NOT NULL)) AS m
INNER JOIN text_hashes AS h ON h.path = m.path
GROUP BY hash_type;

INSERT INTO ham_tresh (hash_type, tresh)
VALUES ('SimHash', 13), ('SimHashWS', 11);

-- TEXT IMAGE HASHES
CREATE TABLE text_image_hashes (
    path TEXT,
    hash_type TEXT,
    hash TEXT,
    PRIMARY KEY (path, hash_type)
);

CREATE TABLE hash_matches_img (
    search_hash TEXT,
    target_hash TEXT,
    search_path TEXT,
    target_path TEXT,
    hash_type TEXT,
    ham INTEGER,
    type TEXT,
    PRIMARY KEY (search_path, target_path, hash_type)
);
CREATE INDEX typeidx ON hash_matches_img(type);


-- different bases

INSERT INTO hash_matches_img (search_hash, target_hash, search_path, target_path, hash_type, ham, type)
SELECT h1.hash, h2.hash, h1.path, h2.path, h1.hash_type, hexhammdist(h1.hash, h2.hash), 'diff_base'
FROM (SELECT h.hash, h.path, h.hash_type, m.origin
    FROM (SELECT origin, path
        FROM memes
        WHERE angle IS NULL
        AND jpg_quality IS NULL
        AND brightness IS NULL
        AND contrast IS NULL
        AND sharpness IS NULL
        AND color IS NULL) as m
    INNER JOIN text_image_hashes as h ON h.path = m.path) AS h1
INNER JOIN (SELECT h.hash, h.path, h.hash_type, m.origin, m.super_origin
    FROM (SELECT path, origin, super_origin
    FROM memes
    WHERE angle IS NOT NULL
        OR jpg_quality IS NOT NULL
        OR brightness IS NOT NULL
        OR contrast IS NOT NULL
        OR sharpness IS NOT NULL
        OR color IS NOT NULL
    LIMIT 20) AS m
    INNER JOIN text_image_hashes as h ON h.path = m.path
    ) AS h2
    ON h1.path != h2.origin
    AND h1.origin != h2.super_origin
    AND h1.hash_type = h2.hash_type;

-- same bases different text
INSERT INTO hash_matches_img (search_hash, target_hash, search_path, target_path, hash_type, ham, type)
SELECT h1.hash, h2.hash, h1.path, h2.path, h1.hash_type, hexhammdist(h1.hash, h2.hash), 'same_base_diff_text'
FROM (SELECT h.hash, h.path, h.hash_type, m.origin
    FROM (SELECT origin, path
        FROM memes
        WHERE angle IS NULL
        AND jpg_quality IS NULL
        AND brightness IS NULL
        AND contrast IS NULL
        AND sharpness IS NULL
        AND color IS NULL) as m
    INNER JOIN text_image_hashes as h ON h.path = m.path) AS h1
INNER JOIN (SELECT h.hash, h.path, h.hash_type, m.origin, m.super_origin
    FROM text_image_hashes AS h
    INNER JOIN memes as m ON h.path = m.path
    WHERE m.angle IS NOT NULL
        OR m.jpg_quality IS NOT NULL
        OR m.brightness IS NOT NULL
        OR m.contrast IS NOT NULL
        OR m.sharpness IS NOT NULL
        OR m.color IS NOT NULL
    ) AS h2
    ON h1.path != h2.origin
    AND h1.origin = h2.super_origin
    AND h1.hash_type = h2.hash_type;

-- correct ones
INSERT INTO hash_matches_img (search_hash, target_hash, search_path, target_path, hash_type, ham, type)
SELECT h1.hash, h2.hash, h1.path, h2.path, h1.hash_type, hexhammdist(h1.hash, h2.hash), 'correct'
FROM (SELECT h.hash, h.path, h.hash_type, m.origin
    FROM (SELECT origin, path
        FROM memes
        WHERE angle IS NULL
        AND jpg_quality IS NULL
        AND brightness IS NULL
        AND contrast IS NULL
        AND sharpness IS NULL
        AND color IS NULL) as m
    INNER JOIN text_image_hashes as h ON h.path = m.path) AS h1
INNER JOIN (SELECT h.hash, h.path, h.hash_type, m.origin, m.super_origin
    FROM text_image_hashes AS h
    INNER JOIN memes as m ON h.path = m.path
    WHERE m.angle IS NOT NULL
        OR m.jpg_quality IS NOT NULL
        OR m.brightness IS NOT NULL
        OR m.contrast IS NOT NULL
        OR m.sharpness IS NOT NULL
        OR m.color IS NOT NULL
    ) AS h2
    ON h1.path = h2.origin
    AND h1.hash_type = h2.hash_type;

SELECT type, COUNT(*) FROM hash_matches_img GROUP BY type, hash_type;

SELECT hash_type, min(ham), round(avg(ham),2), max(ham), count(*)
FROM hash_matches_img
WHERE type='same_base_diff_text'
GROUP BY hash_type;

SELECT hash_type, min(ham), round(avg(ham),2), max(ham), count(*)
FROM hash_matches_img
WHERE type='correct'
GROUP BY hash_type;

SELECT hash_type, min(ham), round(avg(ham),2), max(ham), count(*)
FROM hash_matches_img
WHERE type='diff_base'
GROUP BY hash_type;

DELETE FROM hash_matches_img WHERE type = 'diff_base';

SELECT count(*) FROM (SELECT DISTINCT hash_type FROM text_image_hashes);

select hash from text_image_hashes limit 10;

SELECT * FROM ham_tresh;
