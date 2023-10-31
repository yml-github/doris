WITH handle_final AS (
    SELECT
        windowId,
        aggCondition,
        alarmStatus
    FROM
        securityAlarm.ailpha_security_merge_alarm_handle final ),
     alarm_tmp AS (
         SELECT
             windowId,
             aggCondition,
             maxEndTime,
             minEventId,
             minEndTime,
             _alarmStatus as alarmStatus
         FROM
             (
                 SELECT
                     windowId,
                     aggCondition,
                     max(endTime) AS maxEndTime,
                     min(endTime) AS minEndTime,
                     min(eventId) AS minEventId,
                     any(alarmStatus) as _alarmStatus
                 FROM
                     ailpha_security_alarm
                         left join handle_final
                                   using windowId,
                                         aggCondition
                 WHERE ${whereCondition}
                 GROUP BY
                     windowId,
                     aggCondition
                 ORDER BY
                     maxEndTime DESC
                 LIMIT 0,
                     10 ) AS aa )
 SELECT
    windowId,
    aggCondition,
    _netId as netId,
    _eventId as eventId,
    _startTime as startTime,
    _endTime as endTime,
    _endTime >= today() as isCurrentDay,
    _srcAddress as srcAddress,
    _destAddress as destAddress,
    _srcOrgId as srcOrgId,
    _destOrgId as destOrgId,
    _subCategory as subCategory,
    _alarmName as alarmName,
    if(empty(_threatName)
           OR hasAll(_threatName,
                     ['']),
       _alarmName,
       _threatName) AS threatName,
    if(empty(_threatName)
           OR hasAll(_threatName,
                     ['']),
       _alarmNameCount,
       _threatNameCount) AS threatNameCount,
    if(has(_threatSeverity,
           'High'),
       'High',
       if(has(_threatSeverity,
              'Medium'),
          'Medium',
          'Low')) as threatSeverity,
    _alarmResults as alarmResults,
    _modelName as modelName,
    if(empty(_alarmStatus),
       'unprocessed',
       _alarmStatus) as alarmStatus,
    eventCount,
    srcAddressCount,
    destAddressCount,
    subCategoryCount,
    alarmResultsCount,
    _baasPlatformId as baas_platform_id,
    _baasAlarmUuid as baasAlarmUuid
 FROM
    (
     SELECT
         windowId,
         aggCondition,
         any(minEventId) as _eventId,
         any(minEndTime) as _startTime,
         any(maxEndTime) as _endTime,
         any(alarmStatus) as _alarmStatus,
         toInt64(count(1)) as eventCount,
         any(modelName) as _modelName,
         any(srcOrgId) as _srcOrgId,
         any(destOrgId) as _destOrgId,
         any(baas_platform_id) as _baasPlatformId,
         any(baasAlarmUuid) as _baasAlarmUuid,
         topK(1)(threatName) AS _threatName,
         uniqExact(threatName) AS _threatNameCount,
         topK(1)(alarmName) as _alarmName,
         uniqExact(alarmName) as _alarmNameCount,
         topK(1)(srcAddress) as _srcAddress,
         uniqExact(srcAddress) as srcAddressCount,
         topK(1)(destAddress) as _destAddress,
         uniqExact(destAddress) as destAddressCount,
         topK(1)(subCategory) as _subCategory,
         uniqExact(subCategory) as subCategoryCount,
         groupUniqArray(threatSeverity) as _threatSeverity,
         topK(1)(alarmResults) as _alarmResults,
         uniqExact(alarmResults) as alarmResultsCount,
         groupUniqArray(netId) as _netId
     FROM
         (
          SELECT
              windowId,
              aggCondition,
              startTime,
              endTime,
              eventId,
              baas_platform_id,
              baasAlarmUuid,
              alarmStatus,
              modelName,
              srcOrgId,
              destOrgId,
              alarmName,
              maxEndTime,
              minEndTime,
              minEventId,
              threatName,
              srcAddress,
              destAddress,
              subCategory,
              threatSeverity,
              netId,
              alarmResults
          FROM
              ailpha_security_alarm AS a semi
                  left join alarm_tmp
                            USING windowId,
                                  aggCondition
          WHERE ${whereCondition} )
     GROUP BY
         windowId,
         aggCondition )
 ORDER BY
    endTime desc,
    windowId desc,
    aggCondition desc