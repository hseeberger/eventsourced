CREATE TABLE
  IF NOT EXISTS evts (
    seq_no bigint,
    type text,
    id uuid,
    evt bytea,
    tag text,
    PRIMARY KEY (seq_no, id)
  );

CREATE INDEX IF NOT EXISTS evts_tag ON evts (tag);