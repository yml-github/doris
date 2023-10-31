SELECT
    final_result.windowId,
    final_result.aggCondition,
    final_result._netId as netId,
    final_result._eventId as eventId,
    final_result._startTime as startTime,
    final_result._endTime as endTime,
    final_result._endTime >= CURDATE() as isCurrentDay,
    final_result._srcAddress as srcAddress,
    final_result._destAddress as destAddress,
    final_result._srcOrgId as srcOrgId,
    final_result._destOrgId as destOrgId,
    final_result._subCategory as subCategory,
    final_result._alarmName as alarmName,
    ifnull(final_result._threatName, final_result._alarmName) AS threatName,
    if(final_result._threatSeverity is null,
    ['Low'],
    final_result._threatSeverity) as threatSeverity,
    final_result._alarmResults as alarmResults,
    final_result._modelName as modelName,
    if(final_result._alarmStatus is null,
    'unprocessed',
    final_result._alarmStatus) as alarmStatus,
    final_result.eventCount,
    final_result.srcAddressCount,
    final_result.destAddressCount,
    final_result.subCategoryCount,
    final_result.alarmResultsCount,
    final_result._baasAlarmUuid as baasAlarmUuid
 FROM
    (
    SELECT
    topn_tmp.windowId,
    topn_tmp.aggCondition,
    any_value(topn_tmp.minEventId) as _eventId,
    any_value(topn_tmp.minEndTime) as _startTime,
    any_value(topn_tmp.maxEndTime) as _endTime,
    any_value(topn_tmp.alarmStatus) as _alarmStatus,
    count(1) as eventCount,
    any_value(topn_tmp.modelName) as _modelName,
    any_value(topn_tmp.srcOrgId) as _srcOrgId,
    any_value(topn_tmp.destOrgId) as _destOrgId,
    any_value(topn_tmp.baasAlarmUuid) as _baasAlarmUuid,
    topn(topn_tmp.threatName, 1) AS _threatName,
    count(distinct topn_tmp.threatName) AS _threatNameCount,
    topn(topn_tmp.alarmName, 1) as _alarmName,
    count(distinct topn_tmp.alarmName) as _alarmNameCount,
    topn(topn_tmp.srcAddress, 1) as _srcAddress,
    count(distinct topn_tmp.srcAddress) as srcAddressCount,
    topn(topn_tmp.destAddress, 1) as _destAddress,
    count(distinct topn_tmp.destAddress) as destAddressCount,
    topn(topn_tmp.subCategory, 1) as _subCategory,
    count(distinct topn_tmp.subCategory) as subCategoryCount,
    collect_set(topn_tmp.threatSeverity) as _threatSeverity,
    topn(topn_tmp.alarmResults, 1) as _alarmResults,
    count(distinct topn_tmp.alarmResults) as alarmResultsCount,
    collect_set(topn_tmp.netId) as _netId
    FROM
    (
    SELECT
    asa.windowId,
    asa.aggCondition,
    asa.startTime,
    asa.endTime,
    asa.eventId,
    asa.baas_platform_id,
    asa.baasAlarmUuid,
    alarm_tmp.alarmStatus,
    asa.modelName,
    asa.srcOrgId,
    asa.destOrgId,
    asa.alarmName,
    alarm_tmp.maxEndTime,
    alarm_tmp.minEndTime,
    alarm_tmp.minEventId,
    asa.threatName,
    asa.srcAddress,
    asa.destAddress,
    asa.subCategory,
    asa.threatSeverity,
    asa.netId,
    asa.alarmResults
    FROM
    ailpha_security_alarm AS asa
    LEFT JOIN (
    SELECT
    aa.windowId,
    aa.aggCondition,
    aa.maxEndTime,
    aa.minEventId,
    aa.minEndTime,
    aa._alarmStatus as alarmStatus
    FROM
    (
    SELECT
    asa.windowId,
    asa.aggCondition,
    max(asa.endTime) AS maxEndTime,
    min(asa.endTime) AS minEndTime,
    min(asa.eventId) AS minEventId,
    any(handle_final.alarmStatus) as _alarmStatus
    FROM
    ailpha_security_alarm asa
    LEFT JOIN (
    SELECT
    windowId,
    aggCondition,
    alarmStatus
    FROM
    ailpha_security_merge_alarm_handle fh
    ) handle_final
    ON asa.windowId = handle_final.windowId and asa.aggCondition = handle_final.aggCondition
    WHERE ${whereCondition}
    GROUP BY
    asa.windowId,
    asa.aggCondition
    ORDER BY
    max(endTime) DESC
    LIMIT 0,
    10 ) AS aa ) alarm_tmp
    on
    asa.windowId = alarm_tmp.windowId
    and asa.aggCondition = alarm_tmp.aggCondition
    WHERE ${whereCondition}
    ) topn_tmp
    GROUP BY topn_tmp.windowId, topn_tmp.aggCondition
    ) final_result
 ORDER BY
    final_result._endTime desc,
    final_result.windowId desc,
    final_result.aggCondition desc