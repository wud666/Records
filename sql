SELECT 
	b.biz_subject,
	b.biz_type,
	b.biz_identity,
	b.pay_amount 
FROM (
  	SELECT
    	biz_subject,
    	biz_type,
  		CASE WHEN COUNT(biz_identity) < 3e6 then 0.1 else 3e5/COUNT(biz_identity) END AS thres
  	FROM toy_data_full    
    GROUP BY biz_subject, biz_type 
    ) a
INNER JOIN (
	SELECT
    	biz_subject,
   		biz_type,
    	biz_identity,
    	pay_amount,
    	rand() as bucket
  	FROM
    	mock_table
    	) b
ON a.biz_subject = b.biz_subject 
AND a.biz_type = b.biz_type
WHERE b.bucket <= a.thres;
