SELECT LEVEL, 'test data_'||LEVEL
  FROM DUAL
CONNECT BY LEVEL <= 10000