  SELECT
    cast(cik as number) companyid,
    st.st_name status,
    companyname name,
    ind.in_name industry,
    if(
      SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D'), 
      SPrating, 
      cast(null as string)) sprating, 
    CASE
      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN false
      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN true
      ELSE cast(null as boolean)
      END as islowgrade, 
    ceoname ceo,
    addrline1 addressline1,
    addrline2 addressline2,
    postalcode,
    city,
    stateprovince stateprov,
    country,
    description,
    foundingdate,
    nvl2(lead(pts) OVER (PARTITION BY cik ORDER BY pts), true, false) iscurrent,
    1 batchid,
    date(pts) effectivedate,
    coalesce(
      lead(date(pts))
        OVER (
          PARTITION BY cik
          ORDER BY pts),
      cast('9999-12-31' as date)) enddate
  FROM {{source('finwire','cmp')}} cmp
  JOIN {wh_db}.StatusType st ON cmp.status = st.st_id
  JOIN {wh_db}.Industry ind ON cmp.industryid = ind.in_id