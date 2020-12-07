DROP INDEX index_pol;
DROP INDEX index_date;
DROP INDEX index_sta;
DROP INDEX index_meta;
DROP INDEX index_pol_date;

CREATE INDEX index_pol ON data (pol);
CREATE INDEX index_date ON data (date);
CREATE INDEX index_sta ON data (sta);
CREATE INDEX index_meta ON data (meta);
CREATE INDEX index_pol_date ON data (pol, date);

DROP INDEX index_data;
CREATE UNIQUE INDEX index_data ON data (date, sta, meta, pol);

CREATE INDEX index_pol ON data (date, sta, pol);
VACUUM;