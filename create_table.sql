CREATE TABLE tele_black(
  id int PRIMARY KEY AUTO_INCREMENT,
  company VARCHAR(256) COMMENT '该手机拥有的其中一个公司名',
  tele VARCHAR(11),
  UNIQUE KEY phone_key (tele)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tc58_tele(
  id int PRIMARY KEY AUTO_INCREMENT,
  title VARCHAR(256) COMMENT '该手机发布的最后一个职位',
  company VARCHAR(256) COMMENT '该手机最近一个发布职位的公司',
  city VARCHAR(256) COMMENT '城市',
  tele VARCHAR(11) ,
  publish_date INT(8) COMMENT '该手机发布第一个职位的时间',
  update_date INT(8) COMMENT '该手机发布最后一个职位的时间',
  is_sended INT COMMENT '是否发送过短信，0:还未发送 1:已经发送 2:发送失败',
  is_registed INT COMMENT '是否在发送短信后注册了,0:还未注册 1:以boss身份注册 2:以牛人身份注册 3:未发送就已经在店长注册 4:未发送就已经在Boss注册',
  _storage_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  UNIQUE KEY phone_key (tele)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tc58_city(
  id int PRIMARY KEY AUTO_INCREMENT,
  city VARCHAR(256) COMMENT '城市'
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;