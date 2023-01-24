CREATE TABLE IF NOT EXISTS evts (
  seq_no bigserial,
  id uuid,
  evt bytea,
  tag text,
  PRIMARY KEY (seq_no, id)
);
CREATE INDEX evts_tag ON evts(tag);