CREATE TABLE IF NOT EXISTS spots (
  id INT AUTO_INCREMENT,
  dx VARCHAR(64) NOT NULL,
  freq DECIMAL(10,3) NOT NULL,
  band VARCHAR(64) NOT NULL,
  de VARCHAR(64) NOT NULL,
  mode VARCHAR(64) NULL, 
  db INT NULL,
  timestamp DATETIME NOT NULL,
  speed INT NULL,
  tx_mode VARCHAR(8) NULL,

  PRIMARY KEY (id),
  KEY (dx),
  KEY (band),
  KEY (de),
  KEY (timestamp)
);
