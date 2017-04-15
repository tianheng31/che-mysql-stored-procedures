DELIMITER $$

USE `chezai_new`$$

DROP PROCEDURE IF EXISTS `p_stat_JiJiaSu_byDay`$$

CREATE DEFINER=`cz2016`@`%` PROCEDURE `p_stat_JiJiaSu_byDay`(p_date DATE)

BEGIN
  -- 驾驶行为分析：急加速
  -- 需求：转速大于设定值时，判断车辆在规定时间段的平均加速度是否大于阈值。
  -- 算法：（1）找出车辆加速的运动学片段，过滤后存入临时表tmp03；
  --       （2）对表tmp03进行自连接查询，找出满足条件的组，存入临时表tmp04；
  --       （3）对表tmp04进行排序，去重，将最终结果插入目的表。
  DECLARE v_score INT;
  DECLARE v_time_interval INT;
  DECLARE v_va_speed INT; -- 加速度阈值
  DECLARE v_engineSpeed INT;
  DECLARE v_devid VARCHAR(32);

  DECLARE cur01 CURSOR FOR SELECT deviceId FROM t_vehicleinfo;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET @cursor_not_found = TRUE;

  SET @cursor_not_found := FALSE;

  -- 获取参数
  SELECT para_value INTO v_engineSpeed FROM stat_drivingconfig 
  WHERE type_desc='急加速' AND parameter='engine_speed';

  SELECT para_value INTO v_va_speed FROM stat_drivingconfig
  WHERE type_desc='急加速' AND parameter='va_speed';

  SELECT para_value INTO v_time_interval FROM stat_drivingconfig
  WHERE type_desc='急加速' AND parameter='time_interval';

  SELECT para_value INTO v_score FROM stat_drivingconfig
  WHERE type_desc='急加速' AND parameter='score';

  -- 删除数据，避免多次计算导致重复
  DELETE FROM stat_driving_violation_detail WHERE stat_date=p_date AND actiontype='suddenlySpeedUp';

  -- 打开游标
  OPEN cur01;
  FETCH cur01 INTO v_devid;
  WHILE NOT @cursor_not_found DO
    -- 获取基础数据
    DROP TABLE IF EXISTS tmp01;
    CREATE TEMPORARY TABLE tmp01 (
      VEHICLEDEVICEID VARCHAR(64), -- 车载设备ID
      TIME DATETIME, -- 时间，精确到秒
      speed INT, -- 车速
      gid INT, -- 运动学片段编号
      r INT, -- 片段内数据编号
      pre_time DATETIME,
      pre_speed INT
    );

    INSERT INTO tmp01
    SELECT VEHICLEDEVICEID, TIME, speed,
      CASE WHEN TIMESTAMPDIFF(SECOND,@time,TIME)=1 AND speed>@speed THEN @gid
      -- 判断相邻的车速数据是否为递增，若递增则为同一组，否则产生新组
      ELSE @gid:=@gid+1 END AS gid,
      CASE WHEN TIMESTAMPDIFF(SECOND,@time,TIME)=1 AND speed>@speed THEN @r:=@r+1
      ELSE @r:=1 END AS r,
      @time:=TIME AS pre_time,
      @speed:=speed AS pre_speed
    FROM (
      SELECT a.VEHICLEDEVICEID, a.time, a.speed
      FROM t_vehicleanaloginfo a
      WHERE a.time >= p_date
        AND a.time < p_date + INTERVAL 1 DAY
        AND vehicledeviceid = v_devid
        -- 统计这辆车当天的数据
        AND ENGINE > v_engineSpeed
        AND speed > 0
      ORDER BY TIME ASC ) t1
      INNER JOIN (SELECT @gid:=0,@r:=0,@time:=NULL,@speed:=NULL) t2 ON 1=1;

    -- 过滤掉只有一条数据的运动学片段，从而减少计算量
    DROP TABLE IF EXISTS tmp02;
    CREATE TEMPORARY TABLE tmp02 AS SELECT gid FROM tmp01 WHERE r=2;

    CREATE INDEX idx01 ON tmp01(gid); -- 在gid字段上创建普通索引，加快数据的查询速度

    DROP TABLE IF EXISTS tmp03;
    CREATE TEMPORARY TABLE tmp03 AS
    SELECT * FROM tmp01 WHERE gid IN (SELECT gid FROM tmp02);

    -- 找出符合条件的数据
    DROP TABLE IF EXISTS tmp04;
    CREATE TEMPORARY TABLE tmp04 AS
    SELECT
      a.VEHICLEDEVICEID,
      a.time AS bt,
      a.speed AS bp,
      b.time AS et,
      b.speed AS ep,
      a.gid,
      b.r - a.r + 1 AS total_sec
    FROM
      tmp03 a INNER JOIN tmp03 b -- 自连接
      ON a.gid = b.gid
    WHERE b.time > a.time
      AND b.r - a.r = v_time_interval
      AND (b.speed - a.speed) / (b.r - a.r) >= v_va_speed
    ORDER BY a.gid,
      a.time,
      b.time;

    INSERT INTO stat_driving_violation_detail (
      VehicleDeviceID,
      actiontype,
      actiontypeDesc,
      begin_time,
      end_time,
      stat_date,
      score,
      speed
    )
    SELECT
      VEHICLEDEVICEID,
      'suddenlySpeedUp',
      '急加速',
      bt,
      et,
      p_date,
      v_score,
      ep
    FROM (
      SELECT t1.*,
        CASE WHEN @gid=NULL THEN @r:=1
        WHEN @gid=gid THEN @r:=@r+1
        ELSE @r:=1 END AS r,
        @gid:=gid
      FROM
        (SELECT * FROM tmp04 ORDER BY gid,bt,et) t1
        INNER JOIN (SELECT @gid:=NULL,@r:=1) t2
    ) tt
    WHERE r=1; -- 去掉同一组内重复的急加速记录

    FETCH cur01 INTO v_devid;

  END WHILE;

  -- 关闭游标
  CLOSE cur01;
END$$
DELIMITER;