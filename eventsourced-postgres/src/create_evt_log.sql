CREATE TABLE IF NOT EXISTS evts (
  id uuid,
  seq_no bigint,
  evt bytea,
  PRIMARY KEY (id, seq_no)
);