CREATE TABLE
  IF NOT EXISTS evts (
    seq_no bigint,
    type text,
    id uuid,
    evt bytea,
    PRIMARY KEY (seq_no, id)
  );