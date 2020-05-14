-- now
-- now - predefined number of steps

WITH RECURSIVE dtrange(period) AS (
  VALUES('2015-10-03 00:00:00')

  UNION ALL
  -- SELECT datetime(period, '+6 hour')
  SELECT datetime(period, '+15 minute')
  FROM dtrange
  WHERE period < '2015-10-03 04:00:00'

)
SELECT period FROM dtrange;
