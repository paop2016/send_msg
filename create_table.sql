CREATE TABLE tele_black(
  id int PRIMARY KEY AUTO_INCREMENT,
  company VARCHAR(256) COMMENT '该手机拥有的其中一个公司名',
  tele VARCHAR(11),
  UNIQUE KEY phone_key (tele)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tc58_tele(
  id int PRIMARY KEY AUTO_INCREMENT,
  company VARCHAR(256) COMMENT '该手机拥有的其中一个公司名',
  tele VARCHAR(11) ,
  publish_date INT(8) COMMENT '该手机发布第一个职位的时间',
  update_date INT(8) COMMENT '该手机发布最后一个职位的时间',
  type INT COMMENT '类型，0:文案0 1:文案1',
  is_sended INT COMMENT '是否发送过短信，0:还未发送 1:已经发送',
  is_registed INT COMMENT '是否在发送短信后注册了,0:还未注册 1:注册啦 2:未发送就已经注册',
  _storage_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '最后修改时间',
  UNIQUE KEY phone_key (tele)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tc58_count(
  id int PRIMARY KEY AUTO_INCREMENT,
  company VARCHAR(256) COMMENT '该手机拥有的其中一个公司名',
  tele VARCHAR(11) ,
  publish_date INT(8) COMMENT '该手机发布第一个职位的时间',
  update_date INT(8) COMMENT '该手机发布最后一个职位的时间',
  type INT COMMENT '类型，0:文案0 1:文案1',
  is_sended INT COMMENT '是否发送过短信，0:还未发送 1:已经发送',
  is_registed INT COMMENT '是否在发送短信后注册了,0:还未注册 1:注册啦 2:未发送就已经注册',
  _storage_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '最后修改时间',
  UNIQUE KEY phone_key (tele)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;