CREATE TABLE IF NOT EXISTS snapshots (
  id uuid,
  seq_no bigint,
  state bytea,
  PRIMARY KEY (id, seq_no)
);