DELIMITER $$

USE `chezai_new` $$

DROP PROCEDURE IF EXISTS `p_stat_DaiSuShiChang_byDay` $$

CREATE DEFINER = `cz2016` @`%` PROCEDURE `p_stat_DaiSuShiChang_byDay` (p_date DATE)
BEGIN
  -- 节能分析：怠速时长
  -- 算法描述：发动机转速持续在500-700rpm，累加时间
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

  -- 获取基本数据
  DROP TABLE IF EXISTS tmp01 ;
  CREATE TEMPORARY TABLE tmp01 AS
  SELECT
    a.VEHICLEDEVICEID,
    a.time,
    a.engine
  FROM
    t_vehicleanaloginfo a
  WHERE a.time >= p_date
    AND a.time < ADDDATE(p_date, INTERVAL 1 DAY)
    AND a.engine BETWEEN 500
    AND 700 ;

  DELETE
  FROM
    stat_energy_saving_detail
  WHERE stat_date = p_date
    AND TYPE = 'idleTime' ;

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
    -- 判断两条候选是否连续的，即中间有无其他记录
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
      INSERT INTO stat_energy_saving_detail (
        VehicleDeviceID,
        TYPE,
        typeDesc,
        begin_time,
        end_time,
        stat_date,
        total_seconds
      )
      VALUES
        (
          v_first_vid,
          'idleTime',
          '怠速时长',
          v_first_time,
          v_pre_time,
          p_date,
          TIMESTAMPDIFF(SECOND, v_first_time, v_pre_time)
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