--
-- File generated with SQLiteStudio v3.2.1 on Fri Nov 13 16:45:03 2020
--
-- Text encoding used: UTF-8
--
PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

-- Table: pol

INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (1, 'co', 'Carbon monoxide', NULL, NULL);
INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (2, 'no', 'Nitric oxide', NULL, NULL);
INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (3, 'no2', 'Nitrogen dioxide', NULL, NULL);
INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (4, 'nox', 'Nitrogen oxides', NULL, NULL);
INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (5, 'o3', 'Ozone', NULL, NULL);
INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (6, 'pm10', 'Particulate Matter up to 10 micrometers', NULL, NULL);
INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (7, 'pm25', 'Particulate Matter up to 2.5 micrometers', NULL, NULL);
INSERT INTO pol (id, name, long_name, short_name, unit) VALUES (8, 'so2', 'Sulphur dioxide', NULL, NULL);

COMMIT TRANSACTION;
PRAGMA foreign_keys = on;
