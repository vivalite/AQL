DROP TABLE IF EXISTS faculty;
DROP TABLE IF EXISTS buildings;
DROP TABLE IF EXISTS promotions;

CREATE TABLE faculty (
  person_id TEXT PRIMARY KEY,
  full_name TEXT NOT NULL,
  department TEXT NOT NULL,
  title TEXT NOT NULL,
  building_id TEXT NOT NULL
);

CREATE TABLE buildings (
  building_id TEXT PRIMARY KEY,
  building_name TEXT NOT NULL,
  campus_zone TEXT NOT NULL,
  opened_year INTEGER NOT NULL
);

CREATE TABLE promotions (
  person_id TEXT NOT NULL,
  promoted_to TEXT NOT NULL,
  promotion_date TEXT NOT NULL
);

INSERT INTO faculty VALUES
  ('p001', 'Ada Lovelace', 'Computer Science', 'Professor', 'b001'),
  ('p002', 'Grace Hopper', 'Computer Science', 'Professor', 'b001'),
  ('p003', 'Katherine Johnson', 'Mathematics', 'Professor', 'b002'),
  ('p004', 'Alan Turing', 'Computer Science', 'Associate Professor', 'b003'),
  ('p005', 'Barbara Liskov', 'Computer Science', 'Professor', 'b003');

INSERT INTO buildings VALUES
  ('b001', 'Analytical Engine Hall', 'North', 1984),
  ('b002', 'Orbital Mechanics Center', 'East', 1996),
  ('b003', 'Logic Laboratory', 'North', 2005),
  ('b004', 'Founders Library', 'Central', 1890);

INSERT INTO promotions VALUES
  ('p001', 'Full Professor', '2020-09-01'),
  ('p002', 'Full Professor', '2018-07-15'),
  ('p003', 'Full Professor', '2021-02-01'),
  ('p005', 'Full Professor', '2019-11-20');

