CREATE TABLE
  IF NOT EXISTS events (
    id uuid,
    seq_no bigint,
    type text,
    event bytea,
    PRIMARY KEY (id, seq_no)
  );