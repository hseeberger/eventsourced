CREATE TABLE
  IF NOT EXISTS events (
    seq_no bigint,
    type text,
    id uuid,
    event bytea,
    PRIMARY KEY (seq_no, id)
  );