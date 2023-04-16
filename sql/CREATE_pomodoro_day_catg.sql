CREATE TABLE pomodoro_day_catg (
  date DATE NOT NULL,
  work_minutes NUMERIC NOT NULL,
  learning_minutes NUMERIC NOT NULL,
  CONSTRAINT date_unique UNIQUE (date)
);