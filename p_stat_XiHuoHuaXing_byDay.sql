DELIMITER $$

USE `chezai_new`$$

DROP PROCEDURE IF EXISTS `p_stat_XiHuoHuaXing_byDay`$$

CREATE DEFINER = `cz2016` @`%` PROCEDURE `p_stat_XiHuoHuaXing_byDay` (p_date DATE)
BEGIN
  -- 驾驶行为分析：熄火滑行
  -- 算法描述：空挡有效时，车速大于0，且转速小于300转/分
  DECLARE v_score INT ;
  DECLARE v_engineSpeed INT DEFAULT 300 ;
  DECLARE v_first_vid VARCHAR (256) ;
  DECLARE v_first_time DATETIME ;
  DECLARE v_pre_vid VARCHAR (256) ;
  DECLARE v_pre_time DATETIME ;
  DECLARE v_cur_vid VARCHAR (256) ;
  DECLARE v_cur_time DATETIME ;
  DECLARE v_cnt INT ;
  DECLARE cur01 CURSOR FOR
  SELECT
    VEHICLEDEVICEID,
    TIME
  FROM
    tmp01
  ORDER BY VEHICLEDEVICEID,
    TIME ASC ;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET @cursor_not_found = TRUE ;

  SET @cursor_not_found := FALSE ;

  -- 获取参数
  SELECT
    para_value INTO v_score
  FROM
    stat_drivingconfig
  WHERE type_desc = '熄火滑行'
    AND parameter = 'score' ;
  SELECT
    para_value INTO v_engineSpeed
  FROM
    stat_drivingconfig
  WHERE type_desc = '熄火滑行'
    AND parameter = 'engine_speed' ;

  -- 获取基本数据
  DROP TABLE IF EXISTS tmp01 ;
  CREATE TEMPORARY TABLE tmp01 AS
  SELECT
    a.VEHICLEDEVICEID,
    a.time,
    a.speed,
    a.engine, -- 发动机转速
    b.ngear -- 空挡
  FROM
    t_vehicleanaloginfo a
    INNER JOIN t_vehiclestateinfo1 b
      ON a.time = b.time
      AND a.VEHICLEDEVICEID = b.VEHICLEDEVICEID
  WHERE a.time >= p_date
    AND a.time < ADDDATE(p_date, INTERVAL 1 DAY)
    AND b.time >= p_date
    AND b.time < ADDDATE(p_date, INTERVAL 1 DAY)
    AND a.speed > 0
    AND a.engine < v_engineSpeed
    AND b.ngear = '1' ;

  DELETE
  FROM
    stat_driving_violation_detail
  WHERE stat_date = p_date
    AND actiontype = 'coastingWithEngineOff' ;

  OPEN cur01 ;
  FETCH cur01 INTO v_first_vid,
  v_first_time ;

  SET v_pre_vid := v_first_vid ;
  SET v_pre_time := v_first_time ;

  label01 :
  WHILE
    NOT @cursor_not_found DO
    FETCH cur01 INTO v_cur_vid,
    v_cur_time ;
    IF @cursor_not_found THEN
      -- 此时游标已经取完了，依然再判断一次，否则最后一条数据会被丢掉
      SET v_cur_vid := 'xx' ;
      -- 此时v_cur_time的值等于v_pre_time
    END IF ;
    -- 判断两条候选是否连续，即中间有无其他记录
    SELECT
      COUNT(1) INTO v_cnt
    FROM
      (SELECT
        1
      FROM
        t_vehicleanaloginfo a
      WHERE a.VEHICLEDEVICEID = v_first_vid
        AND a.time > v_pre_time
        AND a.time < v_cur_time
      LIMIT 1) t ;
    IF v_cur_vid <> v_pre_vid OR v_cnt = 1 THEN
      -- 不是同一辆车或相邻的两个候选不是连续的记录
      -- 该次违规判断结束，重新初始化first
      INSERT INTO stat_driving_violation_detail (
        VehicleDeviceID,
        actiontype,
        actiontypeDesc,
        begin_time,
        end_time,
        stat_date,
        score
      )
      VALUES
        (
          v_first_vid,
          'coastingWithEngineOff',
          '熄火滑行',
          v_first_time,
          v_pre_time,
          p_date,
          v_score
        ) ;
      SET v_first_vid := v_cur_vid ;
      SET v_first_time := v_cur_time ;
      SET v_pre_vid := v_first_vid ;
      SET v_pre_time := v_first_time ;
    -- 是连续的，继续判断
    ELSE -- 算违规，但该次违规判断还未结束，继续判断下一条记录
      SET v_pre_time := v_cur_time ;
    END IF ;
  END WHILE label01 ;
  CLOSE cur01 ;
END $$

DELIMITER ;