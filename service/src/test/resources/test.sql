SET MODE MYSQL;
-- ----------------------------
-- Records of data_connector
-- ----------------------------
INSERT INTO `data_connector` VALUES ('1', '2017-07-12 11:06:47', null, '{\"database\":\"default\",\"table.name\":\"data_avr\"}', 'HIVE', '1.2');
INSERT INTO `data_connector` VALUES ('2', '2017-07-12 11:06:47', null, '{\"database\":\"default\",\"table.name\":\"cout\"}', 'HIVE', '1.2');
INSERT INTO `data_connector` VALUES ('3', '2017-07-12 17:40:30', null, '{\"database\":\"griffin\",\"table.name\":\"avr_in\"}', 'HIVE', '1.2');
INSERT INTO `data_connector` VALUES ('4', '2017-07-12 17:40:30', null, '{\"database\":\"griffin\",\"table.name\":\"avr_out\"}', 'HIVE', '1.2');


-- ----------------------------
-- Records of evaluate_rule
-- ----------------------------
--INSERT INTO `evaluate_rule` VALUES ('1', '2017-07-12 11:06:47', null, '$source[\'uid\'] == $target[\'url\'] AND $source[\'uage\'] == $target[\'createdts\']', '0');
INSERT INTO `evaluate_rule` VALUES ('1', '2017-07-12 11:06:47', null, '$source[''uid''] == $target[''url''] AND $source[''uage''] == $target[''createdts'']', '0');

--INSERT INTO `evaluate_rule` VALUES ('2', '2017-07-12 17:40:30', null, '$source[\'id\'] == $target[\'id\'] AND $source[\'age\'] == $target[\'age\'] AND $source[\'desc\'] == $target[\'desc\']', '0');
INSERT INTO `evaluate_rule` VALUES ('2', '2017-07-12 17:40:30', null, '$source[''id''] == $target[''id''] AND $source[''age''] == $target[''age''] AND $source[''desc''] == $target[''desc'']', '0');


-- ----------------------------
-- Records of measure
-- ----------------------------
INSERT INTO `measure` VALUES ('1', '2017-07-12 11:06:47', null, '0', 'desc1', 'buy_rates_hourly', 'eBay', 'test', 'accuracy', '1', '1', '2');
INSERT INTO `measure` VALUES ('2', '2017-07-12 17:40:30', null, '0', 'desc2', 'griffin_aver', 'eBay', 'test', 'accuracy', '2', '3', '4');
