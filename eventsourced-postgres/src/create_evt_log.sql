CREATE TABLE IF NOT EXISTS evts (
  id uuid,
  seq_no bigint,
  "offset" bigserial,
  evt bytea,
  tag text,
  PRIMARY KEY (id, seq_no)
);
CREATE UNIQUE INDEX evts_offset_idx ON evts("offset");
CREATE INDEX evts_tag ON evts(tag);