SELECT
    windowId,
    aggCondition,
    _netId as netId,
    _eventId as eventId,
    _startTime as startTime,
    _endTime as endTime,
    _endTime >= CURDATE() as isCurrentDay,
    _srcAddress as srcAddress,
    _destAddress as destAddress,
    _srcOrgId as srcOrgId,
    _destOrgId as destOrgId,
    _subCategory as subCategory,
    _alarmName as alarmName,
    ifnull(_threatName, _alarmName) AS threatName,
    if(_threatSeverity is null, 'Low', _threatSeverity) as threatSeverity,
    _alarmResults as alarmResults,
    _modelName as modelName,
    if(_alarmStatus is null, 'unprocessed', _alarmStatus) as alarmStatus,
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
    any_value(minEventId) as _eventId,
    any_value(minEndTime) as _startTime,
    any_value(maxEndTime) as _endTime,
    any_value(alarmStatus) as _alarmStatus,
    count(1) as eventCount,
    any_value(modelName) as _modelName,
    any_value(srcOrgId) as _srcOrgId,
    any_value(destOrgId) as _destOrgId,
    any_value(baas_platform_id) as _baasPlatformId,
    any_value(baasAlarmUuid) as _baasAlarmUuid,
    topn(1, threatName) AS _threatName,
    count(distinct threatName) AS _threatNameCount,
    topn(1, alarmName) as _alarmName,
    count(distinct alarmName) as _alarmNameCount,
    topn(1, srcAddress) as _srcAddress,
    count(distinct srcAddress) as srcAddressCount,
    topn(1, destAddress) as _destAddress,
    count(distinct destAddress) as destAddressCount,
    topn(1, subCategory) as _subCategory,
    count(distinct subCategory) as subCategoryCount,
    collect_set(threatSeverity) as _threatSeverity,
    topn(1, alarmResults) as _alarmResults,
    count(distinct alarmResults) as alarmResultsCount,
    collect_set(netId) as _netId
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
    ailpha_security_alarm AS semi
    left join (
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
    any_value(alarmStatus) as _alarmStatus
    FROM
    ailpha_security_alarm alarm_top
    left join (
    SELECT
    windowId,
    aggCondition,
    alarmStatus
    FROM
    securityAlarm.ailpha_security_merge_alarm_handle final ) handle_final on
    alarm_top.windowId = handle_final.windowId
    and alarm_top.aggCondition = handle_final.aggCondition
    WHERE ${whereCondition}
    GROUP BY
    windowId,
    aggCondition
    ORDER BY
    maxEndTime DESC
    LIMIT 10) alarm_tmp
    on
    semi.windowId = alarm_tmp.windowId
    AND semi.aggCondition = alarm_tmp.aggCondition
    WHERE ${whereCondition} )
    GROUP BY
    windowId,
    aggCondition )
    ORDER BY
    endTime desc,
    windowId desc,
    aggCondition desc
    )