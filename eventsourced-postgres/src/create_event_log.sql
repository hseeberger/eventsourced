CREATE TABLE
  IF NOT EXISTS events (
    type text,
    id uuid,
    seq_no bigint,
    event bytea,
    PRIMARY KEY (type, seq_no, id)
  );