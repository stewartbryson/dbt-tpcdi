select
    DATE_VALUE SK_DATE_ID,
	DATE_VALUE,
	DATE_DESC,
	CALENDAR_YEAR_ID,
	CALENDAR_YEAR_DESC,
	CALENDAR_QTR_ID,
	CALENDAR_QTR_DESC,
	CALENDAR_MONTH_ID,
	CALENDAR_MONTH_DESC,
	CALENDAR_WEEK_ID,
	CALENDAR_WEEK_DESC,
	DAY_OF_WEEK_NUM,
	DAY_OF_WEEK_DESC,
	FISCAL_YEAR_ID,
	FISCAL_YEAR_DESC,
	FISCAL_QTR_ID,
	FISCAL_QTR_DESC,
	HOLIDAY_FLAG
from {{ source('reference', 'date') }}