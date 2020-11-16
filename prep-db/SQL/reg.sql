--
-- File generated with SQLiteStudio v3.2.1 on Fri Nov 13 16:45:16 2020
--
-- Text encoding used: UTF-8
--
PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

INSERT INTO reg (id, name, lat, lon, parent_id) VALUES (1, 'marmara', 40.692, 28.3516, NULL);
INSERT INTO reg (id, name, lat, lon, parent_id) VALUES (2, 'ege', 38.6286, 28.6262, NULL);
INSERT INTO reg (id, name, lat, lon, parent_id) VALUES (3, 'akdeniz', 36.4598, 32.9768, NULL);
INSERT INTO reg (id, name, lat, lon, parent_id) VALUES (4, 'iç anadolu', 38.9369, 33.2734, NULL);
INSERT INTO reg (id, name, lat, lon, parent_id) VALUES (5, 'karadeniz', 41.1568, 34.6797, NULL);
INSERT INTO reg (id, name, lat, lon, parent_id) VALUES (6, 'doğu anadolu', 39.2524, 41.2605, NULL);
INSERT INTO reg (id, name, lat, lon, parent_id) VALUES (7, 'güney doğu anadolu', 37.8086, 38.8984, NULL);

COMMIT TRANSACTION;
PRAGMA foreign_keys = on;
